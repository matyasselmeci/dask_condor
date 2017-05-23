"""
Make dask workers using condor
"""
from __future__ import division, print_function

import atexit
import errno
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


logger = logging.getLogger(__name__)

JOB_TEMPLATE = \
    { 'Executable':           '/usr/bin/dask-worker'
    , 'Universe':             'vanilla'
    # $F(MY.JobId) strips the quotes from MY.JobId
    , 'Output':               'worker-$F(MY.JobId).out'
    , 'Error':                'worker-$F(MY.JobId).err'
    , 'Log':                  'worker-$F(MY.JobId).log'

    # using MY.Arguments instead of Arguments lets the value be a classad
    # expression instead of a string. Thanks TJ!
    , 'MY.Arguments':
        'strcat( MY.DaskSchedulerAddress'
        # can't name a worker if we have more than one proc, in which case
        #  we don't need a nanny
        '      , " --nprocs=1"'
        '      , " --no-nanny"'
        '      , " --nthreads=", MY.DaskNThreads'
        '      , " --no-bokeh"'
    # default memory limit is 75% of memory; use 75% of RequestMemory instead
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

JOB_STATUS_IDLE = 1
JOB_STATUS_RUNNING = 2
JOB_STATUS_HELD = 5
HOLD_REASON_PERIODIC_HOLD = 2

SCRIPT_TEMPLATE = """\
#!/bin/bash

if [[ -z $_CONDOR_SCRATCH_DIR ]]; then
    echo '$_CONDOR_SCRATCH_DIR not defined'
    echo 'This script needs to be run as an HTCondor job'
    exit 1
fi

export HOME=$_CONDOR_SCRATCH_DIR

tar xzf ~/%(worker_tarball)s
~/dask_condor_worker/fixpaths.sh
export PATH=~/dask_condor_worker/bin:$PATH
export PYTHONPATH=~:${PYTHONPATH+":$PYTHONPATH"}

args=( "$@" )

# This isn't actually necessary - $TMP is already under $_CONDOR_SCRATCH_DIR
# so that's where mktemp will make its files.
local_directory=$_CONDOR_SCRATCH_DIR/.worker
mkdir -p "$local_directory"
args+=(--local-directory "$local_directory")

exec dask-worker "${args[@]}"
"""


_global_schedulers = [] # (scheduler_id, schedd)


@atexit.register
def global_killall():
    for sid, schedd in _global_schedulers:
        condor_rm(schedd, 'DaskSchedulerId == "%s"' % sid)


def worker_constraint(jobid):
    if '.' in jobid:
        return '(JobId == %s)' % (jobid)
    else:
        return '(ClusterId == %s)' % jobid


def or_constraints(constraints):
    return '(' + ' || '.join(constraints) + ')'


def workers_constraint(jobids):
    return or_constraints([worker_constraint(jid) for jid in jobids])


def workers_constraint_by_name(names):
    return or_constraints(['(DaskWorkerName =?= "%s")' % n for n in names])


def condor_rm(schedd, job_spec):
    return schedd.act(htcondor.JobAction.Remove, job_spec)


def condor_release(schedd, job_spec):
    return schedd.act(htcondor.JobAction.Release, job_spec)


class Error(Exception):
    pass


class HTCondorCluster(object):
    def __init__(self,
                 memory_per_worker=1024,
                 disk_per_worker=1048576,
                 pool=None,
                 schedd_name=None,
                 threads_per_worker=1,
                 update_interval=1000,
                 worker_timeout=(24 * 60 * 60),
                 scheduler_port=8786,
                 worker_tarball=None,
                 transfer_files=None,
                 **kwargs):

        if 'procs_per_worker' in kwargs:
            logger.warning("Multiple processes and adaptive scaling don't mix;"
                           "ignoring procs_per_worker")
        self.procs_per_worker = 1
        self.memory_per_worker = memory_per_worker
        self.disk_per_worker = disk_per_worker
        self.threads_per_worker = threads_per_worker
        if int(update_interval) < 1:
            raise ValueError("update_interval must be >= 1")
        self.worker_timeout = worker_timeout
        self.worker_tarball = worker_tarball
        self.transfer_files = transfer_files

        if schedd_name is None:
            self.schedd = htcondor.Schedd()
        else:
            collector = htcondor.Collector(pool)
            self.schedd = htcondor.Schedd(
                collector.locate(
                    htcondor.DaemonTypes.Schedd,
                    schedd_name))

        self.script = None
        if self.worker_tarball:
            if '://' not in self.worker_tarball:
                self._verify_tarball()
            self.script = tempfile.NamedTemporaryFile(
                suffix='.sh', prefix='dask-worker-wrapper-')
            self.script.write(SCRIPT_TEMPLATE
                % {'worker_tarball': os.path.basename(self.worker_tarball)})
            self.script.flush()

            @atexit.register
            def _erase_script():
                self.script.close()

        self.local_cluster = distributed.LocalCluster(
            ip='', n_workers=0, scheduler_port=scheduler_port, **kwargs)

        global _global_schedulers
        _global_schedulers.append((self.scheduler.id, self.schedd))

        self.jobs = {}  # {jobid: CLASSAD}
        self.ignored_jobs = set()  # set of jobids
        self._update_callback = tornado.ioloop.PeriodicCallback(
            callback=self.update_jobs,
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
        return self.jobs.keys()

    @property
    def scheduler_constraint(self):
        return '(DaskSchedulerId == "%s")' % self.scheduler.id

    @property
    def running_jobs(self):
        return {k:v for k,v in self.jobs.items()
                if v.get('JobStatus') == JOB_STATUS_RUNNING}

    @property
    def idle_jobs(self):
        return {k:v for k,v in self.jobs.items()
                if v.get('JobStatus') == JOB_STATUS_IDLE}

    @property
    def held_jobs(self):
        return {k:v for k,v in self.jobs.items()
                if v.get('JobStatus') == JOB_STATUS_HELD}

    @property
    def timed_out_jobs(self):
        return {k:v for k,v in self.held_jobs
                if v.get('HoldReasonCode') == HOLD_REASON_PERIODIC_HOLD}

    def start_workers(self,
                      n=1,
                      memory_per_worker=None,
                      disk_per_worker=None,
                      procs_per_worker=None,
                      threads_per_worker=None,
                      worker_timeout=None,
                      transfer_files=None,
                      extra_attribs=None):
        n = int(n)
        if n < 1:
            raise ValueError("n must be >= 1")
        if procs_per_worker:
            logger.warning("Multiple processes and adaptive scaling don't mix;"
                           "ignoring procs_per_worker")
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
        transfer_files = transfer_files or self.transfer_files
        if transfer_files:
            if not isinstance(transfer_files, str):
                transfer_files = ', '.join(transfer_files)

        job = htcondor.Submit(JOB_TEMPLATE)
        job['MY.DaskSchedulerAddress'] = '"' + self.scheduler_address + '"'
        job['MY.DaskNProcs'] = "1"
        job['MY.DaskNThreads'] = str(threads_per_worker)
        job['RequestMemory'] = str(memory_per_worker)
        job['RequestDisk'] = str(disk_per_worker)
        job['MY.DaskSchedulerId'] = '"' + self.scheduler.id + '"'
        job['MY.DaskWorkerTimeout'] = str(worker_timeout)
        if self.script:
            job['Executable'] = self.script.name
            job['Transfer_Input_Files'] = self.worker_tarball \
                + (', ' + transfer_files if transfer_files else '')
        else:
            if transfer_files:
                job['Transfer_Input_Files'] = transfer_files

        if extra_attribs:
            job.update(extra_attribs)

        classads = []
        with self.schedd.transaction() as txn:
            clusterid = job.queue(txn, count=n, ad_results=classads)
        logger.info("%d job(s) submitted to cluster %s." % (n, clusterid))
        for ad in classads:
            self.jobs[ad['JobId']] = ad

    def killall(self):
        condor_rm(self.schedd, self.scheduler_constraint)

    def stop_workers(self, worker_ids):
        if isinstance(worker_ids, str):
            worker_ids = [worker_ids]

        logger.info("Removing %d job(s).", len(worker_ids))
        constraint = '%s && %s' % (
            self.scheduler_constraint,
            workers_constraint(worker_ids)
            )

        condor_rm(self.schedd, constraint)

    def update_jobs(self):
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
                        JOB_STATUS_IDLE, JOB_STATUS_RUNNING, JOB_STATUS_HELD):
                    if jobid in self.jobs:
                        new_jobs[jobid] = classad.ClassAd(dict(self.jobs[jobid]))
                        new_jobs[jobid].update(ad)
                    elif jobid not in self.ignored_jobs:
                        logger.warning("Unknown job found: %s, ignoring", jobid)
                        self.ignored_jobs.add(jobid)

            self.jobs = new_jobs
        except RuntimeError as err:
            # timeouts happen. Ignore them.
            if 'Timeout when waiting for remote host' in err.message:
                pass
            else:
                raise

    def close(self):
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
            logger.info("Releasing %d held job(s).", n_to_release)
            condor_release(self.schedd, '%s && %s' % (
                self.scheduler_constraint,
                workers_constraint(jobids_to_release)))
            n_needed -= n_to_release

        if n_needed < 1:
            return

        self.start_workers(n=n_needed)

    def scale_down(self, workers):
        # `workers` is worker addresses
        worker_info = self.scheduler.worker_info
        names = []
        for w in workers:
            try:
                names.append(worker_info[w]['name'])
            except KeyError:
                pass

        if not names:
            return

        condor_rm(self.schedd, '%s && %s' % (
            self.scheduler_constraint,
            workers_constraint_by_name(names)))

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
            if status == JOB_STATUS_IDLE:
                idle += 1
            elif status == JOB_STATUS_RUNNING:
                running += 1
            elif status == JOB_STATUS_HELD:
                held += 1

        return "<%s: %d workers (%d running, %d idle, %d held)>" \
               % (self.__class__.__name__, total, running, idle, held)

    __repr__ = __str__
