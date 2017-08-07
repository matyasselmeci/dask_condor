"""
Make dask workers using condor
"""
from __future__ import absolute_import, division, print_function

import atexit
import collections
import errno
import itertools
import logging
import os
import tempfile
import time

import distributed
import tornado

import htcondor
import classad

if not hasattr(htcondor, 'Submit'):
    raise ImportError("htcondor.Submit not found;"
                      " HTCondor 8.6.0 or newer required")

from . import util


JOB_TEMPLATE = \
    { 'Executable':           '/usr/bin/dask-worker'
    , 'Universe':             'vanilla'
    # $F(MY.JobId) strips the quotes from MY.JobId
    , 'Output':               '$(LogDir)/worker-$F(MY.JobId).out'
    ## Don't transfer stderr -- we're redirecting it to stdout
    #, 'Error':                '$(LogDir)/worker-$F(MY.JobId).err'
    , 'Log':                  '$(LogDir)/worker-$(ClusterId).log'
    # We kill all the workers to stop them so we need to stream their
    # stdout if we ever want to see anything
    , 'Stream_Output':        'True'
    #, 'Stream_Error':         'True'

    # using MY.Arguments instead of Arguments lets the value be a classad
    # expression instead of a string. Thanks TJ!
    , 'MY.Arguments':
        'strcat( MY.DaskSchedulerAddress'
        # can't name a worker if we have more than one proc, in which case
        # we don't need a nanny
        '      , " --nprocs=1"'
        '      , " --no-nanny"'
        '      , " --nthreads=", MY.DaskNThreads'
        '      , " --no-bokeh"'
        # default memory limit is 75% of memory;
        # use 75% of RequestMemory instead
        '      , " --memory-limit="'
        '      , floor(RequestMemory * 1048576 * 0.75)'
        '      , " --name=", MY.DaskWorkerName'
        '      )'

    , 'MY.DaskWorkerName':    '"htcondor-$F(MY.JobId)"'

    , 'RequestCpus':          'MY.DaskNThreads'

    , 'Periodic_Hold':
        '((time() - EnteredCurrentStatus) > MY.DaskWorkerTimeout) &&'
        ' (JobStatus == 2)'
    , 'Periodic_Hold_Reason': 'strcat("dask-worker exceeded max lifetime of ",'
                              '       interval(MY.DaskWorkerTimeout))'
    , 'Transfer_Input_Files': ''
    , 'MY.JobId':             '"$(ClusterId).$(ProcId)"'
    }

SCRIPT_TEMPLATE = """\
#!/bin/bash

exec -- 2>&1

if [[ -z $_CONDOR_SCRATCH_DIR ]]; then
    echo '$_CONDOR_SCRATCH_DIR not defined'
    echo 'This script needs to be run as an HTCondor job'
    exit 1
fi

export HOME=$_CONDOR_SCRATCH_DIR

if [[ -n "%(worker_tarball)s" ]]; then
    tar xzf ~/"%(worker_tarball)s"
    ~/dask_condor_worker/fixpaths.sh
    export PATH=~/dask_condor_worker/bin:$PATH
    export PYTHONPATH=~:${PYTHONPATH+":$PYTHONPATH"}
fi

args=( "$@" )

# This isn't actually necessary - $TMP is already under $_CONDOR_SCRATCH_DIR
# so that's where mktemp will make its files.
local_directory=$_CONDOR_SCRATCH_DIR/.worker
mkdir -p "$local_directory"
args+=(--local-directory "$local_directory")
if [[ -n "%(pre_script)s" ]]; then
    chmod +x "%(pre_script)s"
    "%(pre_script)s"
fi
exec dask-worker "${args[@]}"
"""


_global_schedulers = []  # (scheduler_id, schedd)


@atexit.register
def global_killall():
    """Kill all worker jobs connected to known schedulers.

    """
    for sid, schedd in _global_schedulers:
        util.condor_rm(schedd, 'DaskSchedulerId == "%s"' % sid)


class Error(Exception):
    pass


class HTCondorCluster(object):
    """
    Cluster manager for workers run as HTCondor jobs.

    Runs a `distributed.scheduler.Scheduler` listening on the local
    machine, and contains methods for launching and managing
    ``dask-worker`` processes that are submitted as HTCondor jobs and run
    remotely.  Starts up a `Scheduler` immediately (via
    `distributed.LocalCluster`) but does not submit any worker jobs until
    told to do so (usually by `start_workers`).

    It is not necessary to use a shared filesystem or pre-distribute
    Python, Dask, Python libraries, or data to the execute nodes before
    sending Dask.distributed jobs to them: `HTCondorCluster` can send a
    "worker tarball" that contains the Python environment, and extra
    files (e.g. data) as desired.

    The "worker tarball" must have a directory called
    ``dask_condor_worker`` with at least the following contents:

    - a Python installation (with interpreter in ``bin/python``)
    - Dask and Dask.distributed installed (e.g. using pip) such that the
      ``dask-worker`` executable is located in ``bin/``
    - a script called ``fixpaths.sh`` that makes sure paths embedded in
      files point to the correct absolute path under the execute
      directory


    Parameters
    ----------
    memory_per_worker, disk_per_worker, threads_per_worker,
    worker_timeout, transfer_files : optional
        Default parameters for worker jobs.  See `start_workers` for
        a description.
    pool : str, optional
        An IP address:port pair of the HTCondor collector to query to
        find out how to contact the HTCondor schedd.  Only used if
        `schedd_name` is also specified.  If not specified, the value of
        the HTCondor configuration variable ``COLLECTOR_HOST`` is used.
    schedd_name : str, optional
        The location (IP address:port) of the schedd to submit jobs to.
        If not specified, the local schedd is used.
    update_interval : int, optional
        In milliseconds, how often to query the schedd to update classad
        information of the worker jobs.
    worker_tarball : str or None, optional
        The path to a tarball to upload that contains the Dask worker
        and the Python environment needed to run it.  See above for the
        requirements.  If not specified or None, no tarball is transferred
        and ``dask-worker`` is run from ``$PATH``.
    pre_script : str or None, optional
        The path to a script that should be run on the remote node after
        unpacking the worker tarball (if there is one), but before
        executing ``dask-worker``.  Use this if you need to prepare data
        (e.g. extract transferred files).  If not specified or None, no
        extra script is run.
    logdir : str, optional
        The path to a directory to place HTCondor job output and logs
        into.  One log file (containing stdout and stderr) is generated
        per HTCondor job, plus another log file (containing job status)
        per HTCondor cluster, so it might be worth placing them into a
        separate directory.  The directory will be created if it does not
        exist.  If not specified, the current directory is used.
    logger : `logging.Logger` or None, optional
        A standard Python `Logger` object to write logs to.  If not
        specified, the logger for this module is used.
    diagnostics_port : int, optional
        The port to start the Bokeh server on for monitoring the
        scheduler.  Unlike LocalCluster, this is disabled by default.
        The typical port is 8787.

    Other parameters are passed into `distributed.LocalCluster` as-is.

    """
    def __init__(self,
                 memory_per_worker=1024,
                 disk_per_worker=1048576,
                 pool=None,
                 schedd_name=None,
                 threads_per_worker=1,
                 update_interval=10000,
                 worker_timeout=(24 * 60 * 60),
                 scheduler_port=8786,
                 worker_tarball=None,
                 pre_script=None,
                 transfer_files=None,
                 logdir='.',
                 logger=None,
                 diagnostics_port=None,
                 **kwargs):

        self.logger = logger or logging.getLogger(__name__)
        self.memory_per_worker = memory_per_worker
        self.disk_per_worker = disk_per_worker
        self.threads_per_worker = threads_per_worker
        if int(update_interval) < 1:
            raise ValueError("update_interval must be >= 1")
        self.worker_timeout = worker_timeout
        self.worker_tarball = worker_tarball
        self.pre_script = pre_script
        self.transfer_files = transfer_files

        if schedd_name is None:
            self.schedd = htcondor.Schedd()
        else:
            collector = htcondor.Collector(pool)
            self.schedd = htcondor.Schedd(
                collector.locate(
                    htcondor.DaemonTypes.Schedd,
                    schedd_name))

        self.script = tempfile.NamedTemporaryFile(
            suffix='.sh', prefix='dask-worker-wrapper-')
        os.chmod(self.script.name, 0o755)
        worker_tarball_in_wrapper = ""
        pre_script_in_wrapper = ""
        if self.worker_tarball:
            if '://' not in self.worker_tarball:
                self._verify_tarball()
            worker_tarball_in_wrapper = os.path.basename(self.worker_tarball)
        if self.pre_script:
            pre_script_in_wrapper = "./" + os.path.basename(self.pre_script)
        self.script.write(SCRIPT_TEMPLATE
            % {'worker_tarball': worker_tarball_in_wrapper,
               'pre_script': pre_script_in_wrapper})
        self.script.flush()

        @atexit.register
        def _erase_script():
            self.script.close()

        self.logdir = logdir
        try:
            os.makedirs(self.logdir)
        except OSError as err:
            if err.errno == errno.EEXIST:
                pass
            else:
                self.logger.warning("Couldn't make log dir: %s", err)

        self.local_cluster = distributed.LocalCluster(
            ip='', n_workers=0, scheduler_port=scheduler_port,
            diagnostics_port=diagnostics_port, **kwargs)

        # dask-scheduler cannot distinguish task failure from
        # job removal/preemption. This might be a little extreme...
        self.scheduler.allowed_failures = 99999

        global _global_schedulers
        _global_schedulers.append((self.scheduler.id, self.schedd))

        self.jobs = {}  # {jobid: CLASSAD}
        self.ignored_jobs = set()  # set of jobids
        self._update_callback = tornado.ioloop.PeriodicCallback(
            callback=self._update_jobs,
            callback_time=update_interval,
            io_loop=self.scheduler.loop)
        self._update_callback.start()


    @tornado.gen.coroutine
    def _start(self):
        pass

    @property
    def scheduler(self):
        return self.local_cluster.scheduler

    @property
    def scheduler_address(self):
        return self.scheduler.address

    @property
    def jobids(self):
        return list(self.jobs.keys())

    @property
    def scheduler_constraint(self):
        return '(DaskSchedulerId == "%s")' % self.scheduler.id

    @property
    def running_jobs(self):
        return {k:v for k,v in self.jobs.items()
                if v.get('JobStatus') == util.JOB_STATUS_RUNNING}

    @property
    def idle_jobs(self):
        return {k:v for k,v in self.jobs.items()
                if v.get('JobStatus') == util.JOB_STATUS_IDLE}

    @property
    def held_jobs(self):
        return {k:v for k,v in self.jobs.items()
                if v.get('JobStatus') == util.JOB_STATUS_HELD}

    @property
    def timed_out_jobs(self):
        return {k:v for k,v in self.held_jobs.items()
                if v.get('HoldReasonCode') == util.HOLD_REASON_PERIODIC_HOLD}

    def start_workers(self,
                      n=1,
                      memory_per_worker=None,
                      disk_per_worker=None,
                      threads_per_worker=None,
                      worker_timeout=None,
                      transfer_files=None,
                      extra_attribs=None):
        """Start `n` worker jobs in a single HTCondor job cluster.

        The default values for the parameters are instance variables but
        may be overridden here.

        Parameters
        ----------
        n : int, optional
            Number of worker jobs to start (default 1).
        memory_per_worker : int, optional
            Memory (in MB) to request for each worker job.
        disk_per_worker : int, optional
            Disk space (in KB) to request for each worker job.
        threads_per_worker : int, optional
            Number of threads to use in each worker.  Raising this
            increases the number of cores requested for the job.
        worker_timeout : int, optional
            Maximum running time of a worker job, in seconds.  After this,
            the job is held.
        transfer_files : str or list of str, optional
            Additional files (not including the worker tarball) to
            transfer as part of a worker job.  If a str, should be
            comma-separated.
        extra_attribs: dict of str, optional
            Additional submit file attributes to include in a worker
            job.  Will be added to the htcondor.Submit object as-is.

        """
        n = int(n)
        if n < 1:
            raise ValueError("n must be >= 1")
        memory_per_worker = int(memory_per_worker or self.memory_per_worker)
        if memory_per_worker < 1:
            raise ValueError("memory_per_worker must be >= 1 (MB)")
        disk_per_worker = int(disk_per_worker or self.disk_per_worker)
        if disk_per_worker < 1:
            raise ValueError("disk_per_worker must be >= 1 (KB)")
        threads_per_worker = int(threads_per_worker or self.threads_per_worker)
        if threads_per_worker < 1:
            raise ValueError("threads_per_worker must be >= 1")
        worker_timeout = int(worker_timeout or self.worker_timeout)
        if worker_timeout < 1:
            raise ValueError("worker_timeout must be >= 1 (sec)")
        transfer_files = transfer_files or self.transfer_files or []
        if isinstance(transfer_files, str):
            transfer_files = [x.strip() for x in transfer_files.split(',')]

        job = htcondor.Submit(JOB_TEMPLATE)
        job['MY.DaskSchedulerAddress'] = '"' + self.scheduler_address + '"'
        job['MY.DaskNProcs'] = "1"
        job['MY.DaskNThreads'] = str(threads_per_worker)
        job['RequestMemory'] = str(memory_per_worker)
        job['RequestDisk'] = str(disk_per_worker)
        job['MY.DaskSchedulerId'] = '"' + self.scheduler.id + '"'
        job['MY.DaskWorkerTimeout'] = str(worker_timeout)
        job['LogDir'] = self.logdir
        job['Executable'] = self.script.name
        if self.worker_tarball:
            transfer_files.append(self.worker_tarball)
        if self.pre_script:
            transfer_files.append(self.pre_script)
        job['Transfer_Input_Files'] = ', '.join(transfer_files)

        if extra_attribs:
            job.update(extra_attribs)

        classads = []
        with self.schedd.transaction() as txn:
            clusterid = job.queue(txn, count=n, ad_results=classads)
        self.logger.info("%d job(s) submitted to cluster %s." % (n, clusterid))
        for ad in classads:
            self.jobs[ad['JobId']] = ad

    def killall(self):
        """Remove all workers.

        """
        util.condor_rm(self.schedd, self.scheduler_constraint)

    def stop_workers(self, worker_ids):
        """Remove specific workers.

        Parameters
        ----------
        worker_ids : str or list of str
            One or more HTCondor job or cluster IDs that correspond to
            workers started by this HTCondorCluster.

        """
        if isinstance(worker_ids, str):
            worker_ids = [worker_ids]

        self.logger.info("Removing %d job(s).", len(worker_ids))
        constraint = '%s && %s' % (
            self.scheduler_constraint,
            util.workers_constraint(worker_ids)
            )

        util.condor_rm(self.schedd, constraint)

    def _update_jobs(self):
        """Update the classad information about known workers.

        """
        # Don't want to leave the original in a half-updated state
        new_jobs = {}
        try:
            ads = self.schedd.xquery(
                self.scheduler_constraint,
                projection=['JobId', 'JobStatus', 'EnteredCurrentStatus',
                            'HoldReasonCode'])
            for ad in ads:
                jobid = ad['JobId']
                if isinstance(jobid, classad.ExprTree):
                    jobid = jobid.eval()
                jobstatus = ad['JobStatus']
                if isinstance(jobstatus, classad.ExprTree):
                    jobstatus = jobstatus.eval()
                if jobstatus in (
                        util.JOB_STATUS_IDLE, util.JOB_STATUS_RUNNING, util.JOB_STATUS_HELD):
                    if jobid in self.jobs:
                        new_jobs[jobid] = classad.ClassAd(
                            dict(self.jobs[jobid]))
                        new_jobs[jobid].update(ad)
                    elif jobid not in self.ignored_jobs:
                        self.logger.warning("Unknown job found: %s, ignoring",
                                            jobid)
                        self.ignored_jobs.add(jobid)

            self.jobs = new_jobs
        except RuntimeError as err:
            # timeouts happen. Ignore them.
            if 'Timeout when waiting for remote host' in err.message:
                pass
            else:
                raise

    def close(self):
        """Shut off the cluster and all workers.

        """
        self.killall()
        self.local_cluster.close()
        if self.script:
            self.script.close()

    def _verify_tarball(self):
        if not os.path.exists(self.worker_tarball):
            raise IOError(errno.ENOENT, "worker tarball not found",
                          self.worker_tarball)

        import tarfile
        tar = tarfile.open(self.worker_tarball)
        try:
            members = tar.getnames()
            for path in ['dask_condor_worker/fixpaths.sh',
                         'dask_condor_worker/bin/python',
                         'dask_condor_worker/bin/dask-worker']:
                if path not in members:
                    raise Error("Expected file %s not in tarball" % (path))
        finally:
            tar.close()

    def scale_up(self, n):
        """Ensure we have at least `n` workers available to run in the queue.

        The worker jobs may be in 'idle' or 'running' state.  If any
        worker jobs are in the 'held' state due to hitting their timeout,
        they will be released (returned to 'idle' state).

        Parameters
        ----------
        n
            The number of workers.

        Notes
        -----
        This is primarily to satisfy the Adaptive interface.

        """
        n_idle_jobs = len(self.idle_jobs)
        n_running_jobs = len(self.running_jobs)
        timed_out_jobids = list(self.timed_out_jobs.keys())
        n_timed_out_jobs = len(timed_out_jobids)

        n_needed = n - n_idle_jobs - n_running_jobs
        if n_needed < 1:
            # we have enough workers
            return

        if n_timed_out_jobs > 0:
            n_to_release = min([n_needed, n_timed_out_jobs])
            jobids_to_release = timed_out_jobids[:n_to_release]
            self.logger.info("Releasing %d held job(s).", n_to_release)
            util.condor_release(self.schedd, '%s && %s' % (
                self.scheduler_constraint,
                util.workers_constraint(jobids_to_release)))
            n_needed -= n_to_release

        if n_needed < 1:
            return

        self.start_workers(n=n_needed)

    def scale_down(self, workers):
        """Shut off specific workers by IP:port address.

        Parameters
        ----------
        workers : list of str
            Worker IP:port addresses

        Notes
        -----
        This is primarily to satisfy the Adaptive interface.

        """
        # `workers` is worker addresses
        worker_info = self.scheduler.worker_info
        names = []
        for w in workers:
            try:
                names.append(str(worker_info[w]['name']))
            except KeyError:
                pass

        if not names:
            return

        self.logger.info("Removing %d job(s).", len(names))
        util.condor_rm(self.schedd, '%s && %s' % (
            self.scheduler_constraint,
            util.workers_constraint_by_name(names)))

    def stats(self):
        """Useful statistics about the scheduler.

        Returns
        -------
        stats : OrderedDict of ints
            Several statistics which may be of use for checking the
            scheduler status.

        """
        sch = self.scheduler
        return collections.OrderedDict([
              ('condor_running', len(self.running_jobs))
            , ('condor_idle', len(self.idle_jobs))
            , ('tot_ncores', sum(sch.ncores.values()))
            , ('ntasks', len(sch.tasks))
            , ('nfutures', len(sch.who_has))
            , ('nidle', len(sch.idle))
            , ('nprocessing', len(list(itertools.chain.from_iterable(
                list(sch.processing.values())))))
            , ('tot_occupancy', sum(sch.occupancy.values()))
            ])

    def print_stats(self):
        """Print statistics about the scheduler.

        """
        for k,v in self.stats().items():
            print("%-20s: %5s" % (k,v))

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __str__(self):
        total = running = idle = held = 0
        for key in self.jobs:
            status = self.jobs[key]['JobStatus']
            total += 1
            if status == util.JOB_STATUS_IDLE:
                idle += 1
            elif status == util.JOB_STATUS_RUNNING:
                running += 1
            elif status == util.JOB_STATUS_HELD:
                held += 1

        return "<%s: %d workers (%d running, %d idle, %d held)>" \
               % (self.__class__.__name__, total, running, idle, held)

    __repr__ = __str__
