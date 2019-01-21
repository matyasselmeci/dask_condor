"""
Make dask workers using condor
"""
from __future__ import absolute_import, division, print_function

import atexit
import collections
import errno
import itertools
import logging
import math
import os
import re
import tempfile

import dask
import dask_jobqueue
import tornado.ioloop
from distributed.utils import parse_bytes
import htcondor
import classad


from . import util


JOB_TEMPLATE = \
    { 'Universe':             'vanilla'
    # $F(MY.JobId) strips the quotes from MY.JobId
    , 'Output':               '$(LogDirectory)/worker-$F(MY.JobId).out'
    , 'Log':                  '$(LogDirectory)/worker-$(ClusterId).log'
    # We kill all the workers to stop them so we need to stream their
    # stdout if we ever want to see anything
    , 'Stream_Output':        'True'

    , 'MY.DaskWorkerName':    '"htcondor--$F(MY.JobId)--"'
    , 'RequestCpus':          'MY.DaskWorkerCores'
    , 'RequestMemory':        'floor(MY.DaskWorkerMemory / 1048576)'
    , 'RequestDisk':          'floor(MY.DaskWorkerDisk / 1024)'
    # Need to transfer wrapper script at least
    , 'ShouldTransferFiles':  'YES'

    , 'MY.JobId':             '"$(ClusterId).$(ProcId)"'
    }

WRAPPER_SCRIPT = """\
#!/bin/bash

exec -- 2>&1

if [[ -z $_CONDOR_SCRATCH_DIR ]]; then
    echo '$_CONDOR_SCRATCH_DIR not defined'
    echo 'This script needs to be run as an HTCondor job'
    exit 1
fi

cd "$_CONDOR_SCRATCH_DIR"

worker_tarball=$1
pre_script=$2
shift 2


if [[ $worker_tarball ]]; then
    tar xzf "$worker_tarball"
    fixpaths=$_CONDOR_SCRATCH_DIR/dask_condor_worker/fixpaths.sh
    if [[ -f $fixpaths ]]; then
        chmod +x "$fixpaths"
        "$fixpaths"
    fi
    export PATH=$_CONDOR_SCRATCH_DIR/dask_condor_worker/bin:$PATH
    export PYTHONPATH=$_CONDOR_SCRATCH_DIR${PYTHONPATH+":$PYTHONPATH"}
fi

if [[ $pre_script ]]; then
    chmod +x "$pre_script"
    "$pre_script"
fi

exec "$@"
"""


class Error(Exception):
    pass


class HTCondorJQCluster(dask_jobqueue.JobQueueCluster):
    """
    Cluster manager for workers run as HTCondor jobs.

    It is not necessary to use a shared filesystem or pre-distribute
    Python, Dask, Python libraries, or data to the execute nodes before
    sending Dask.distributed jobs to them: `HTCondorJQCluster` can send a
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
    disk : int, kilobytes
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
    logger : `logging.Logger` or None, optional
        A standard Python `Logger` object to write logs to.  If not
        specified, the logger for this module is used.
    diagnostics_port : int, optional
        The port to start the Bokeh server on for monitoring the
        scheduler.  Unlike LocalCluster, this is disabled by default.
        The typical port is 8787.

    Other parameters are passed into `dask_jobqueue.JobQueueCluster` as-is.

    """

    job_id_regexp=r'(?P<job_id>\d+\.\d+)'

    def __init__(self,
                 disk=None,
                 pool=None,
                 schedd_name=None,
                 update_interval=10000,
                 worker_tarball=None,
                 pre_script=None,
                 transfer_files=None,
                 config_name='htcondor',
                 env_extra=None,
                 logger=None,
                 **kwargs):

        # TODO Handle death_timeout and extra (plain extra args)

        if disk is None:
            disk = dask.config.get('jobqueue.%s.disk' % config_name, None)
        if disk is None:
            raise ValueError("You must specify how much disk to use per job like ``disk='1 GB'``")
        self.worker_disk = parse_bytes(disk)

        super(HTCondorJQCluster, self).__init__(config_name=config_name, **kwargs)

        if env_extra is None:
            env_extra = dask.config.get('jobqueue.%s.env-extra' % config_name, default=[])
        if env_extra is None:
            self.env = []
        else:
            # env_extra is an array of export statements
            self.env = [re.sub("^export\\s+", "", e) for e in env_extra if e]

        if not self.worker_processes:
            self.worker_processes = 1
        self.logger = logger or logging.getLogger(__name__)
        if int(update_interval) < 1:
            raise ValueError("update_interval must be >= 1")
        self.worker_tarball = worker_tarball
        self.pre_script = pre_script
        self.transfer_files = transfer_files

        if schedd_name is None:
            schedd_name = dask.config.get('jobqueue.%s.schedd-name' % config_name, None)
        if schedd_name is None:
            self.schedd = htcondor.Schedd()
        else:
            if pool is None:
                pool = dask.config.get('jobqueue.%s.pool' % config_name)
            collector = htcondor.Collector(pool)
            self.schedd = htcondor.Schedd(
                collector.locate(
                    htcondor.DaemonTypes.Schedd,
                    schedd_name))

        self.log_directory = kwargs.get('log_directory')
        if self.log_directory is None:
            self.log_directory = dask.config.get('jobqueue.%s.log-directory' % config_name)
        if self.log_directory is None:
            self.log_directory = "."

        self.script = tempfile.NamedTemporaryFile(
            suffix='.sh', prefix='dask-worker-wrapper-')
        os.chmod(self.script.name, 0o755)
        self.worker_tarball_in_wrapper = ""
        self.pre_script_in_wrapper = ""
        if self.worker_tarball:
            if '://' not in self.worker_tarball:
                self._verify_tarball()
            self.worker_tarball_in_wrapper = os.path.basename(self.worker_tarball)
        if self.pre_script:
            self.pre_script_in_wrapper = "./" + os.path.basename(self.pre_script)
        self.script.write(WRAPPER_SCRIPT)
        self.script.flush()

        @atexit.register
        def _erase_script():
            self.script.close()

        # dask-scheduler cannot distinguish task failure from
        # job removal/preemption. This might be a little extreme...
        self.scheduler.allowed_failures = 99999

        self.jobs = {}  # {jobid: CLASSAD}
        self.ignored_jobs = set()  # set of jobids
        self._update_callback = tornado.ioloop.PeriodicCallback(
            callback=self._update_jobs,
            callback_time=update_interval)
        self._update_callback.start()

    def job_script(self):
        raise NotImplementedError("Conversion to submit file not implemented yet")

    def job_file(self):
        raise NotImplementedError("Conversion to submit file not implemented yet")

    @property
    def jobids(self):
        return self.jobs.keys()

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
                      transfer_files=None,
                      extra_attribs=None):
        """ Start workers and point them to our local scheduler """
        self.logger.debug('starting %s workers', n)
        num_jobs = int(math.ceil(n / self.worker_processes))
        job = htcondor.Submit(JOB_TEMPLATE)
        job['MY.DaskSchedulerAddress'] = '"%s"' % self.scheduler.address
        job['MY.DaskWorkerDisk'] = str(self.worker_disk)
        job['MY.DaskWorkerProcesses'] = str(self.worker_processes)
        job['MY.DaskWorkerProcessThreads'] = str(self.worker_process_threads)
        job['MY.DaskWorkerProcessMemory'] = str(self.worker_process_memory)
        job['MY.DaskWorkerCores'] = str(self.worker_cores)
        job['MY.DaskSchedulerId'] = '"%s"' % self.scheduler.id
        job['MY.DaskWorkerTimeout'] = str(self.worker_timeout)  # TODO

        job['LogDirectory'] = self.log_directory
        job['Executable'] = self.script.name
        job['Arguments'] = util.quote_arguments(
            self.worker_tarball_in_wrapper, self.pre_script_in_wrapper,
            self._command_template
        )
        job['Environment'] = util.quote_environment(["JOB_ID=$F(MY.JobId)"] + self.env)

        transfer_files = transfer_files or self.transfer_files or []
        if isinstance(transfer_files, str):
            transfer_files = [x.strip() for x in transfer_files.split(',')]
        if self.worker_tarball:
            transfer_files.append(self.worker_tarball)
        if self.pre_script:
            transfer_files.append(self.pre_script)
        job['Transfer_Input_Files'] = ', '.join(transfer_files)

        if extra_attribs:
            job.update({k: str(v) for k, v in extra_attribs.items()})

        classads = []
        with self.schedd.transaction() as txn:
            clusterid = job.queue(txn, count=num_jobs, ad_results=classads)
        self.logger.info("%d job(s) submitted to cluster %s." % (num_jobs, clusterid))
        for ad in classads:
            self.jobs[ad['JobId']] = ad
            self.pending_jobs[ad['JobId']] = {}

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
            for path in ['dask_condor_worker/bin/python']:
                if path not in members:
                    raise Error("Expected file %s not in tarball" % (path))
        finally:
            tar.close()

    def scale_up(self, n, **kwargs):
        # TODO
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
        timed_out_jobids = self.timed_out_jobs.keys()
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
                sch.processing.values()))))
            , ('tot_occupancy', sum(sch.occupancy.values()))
            ])

    def print_stats(self):
        """Print statistics about the scheduler.

        """
        for k,v in self.stats().items():
            print("%-20s: %5s" % (k,v))

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
