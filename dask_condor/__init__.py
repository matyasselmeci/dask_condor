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
    , 'Output':               'worker-$(ClusterId).$(ProcId).out'
    , 'Error':                'worker-$(ClusterId).$(ProcId).err'
    , 'Log':                  'worker-$(ClusterId).$(ProcId).log'

    # using MY.Arguments instead of Arguments lets the value be a classad
    # expression instead of a string. Thanks TJ!
    , 'MY.Arguments':
        'strcat( MY.DaskSchedulerAddress'
        '      , " --nprocs=", MY.DaskNProcs'
        '      , " --nthreads=", MY.DaskNThreads'
        '      , " --no-bokeh"'
    # default memory limit is 75% of memory; use 75% of RequestMemory instead
        '      , " --memory-limit="'
        '      , floor(RequestMemory * 1048576 * 0.75)'
    # no point in having a nanny if we're only running 1 proc
        '      , ifThenElse( (MY.DaskNProcs < 2)'
        '                  , " --no-nanny "'
        '                  , "")'
    # we can only have a worker name if nprocs == 1
        '      , ifThenElse( isUndefined(MY.DaskWorkerName)'
        '                  , ""'
        '                  , strcat(" --name=", MY.DaskWorkerName))'
        '      )'

    , 'MY.DaskWorkerName':    'ifThenElse( (MY.DaskNProcs < 2)'
                              '          , "htcondor-$(ClusterId).$(ProcId)"'
                              '          , UNDEFINED)'

    # reserve a CPU for the nanny process if nprocs > 1
    , 'RequestCpus':          'ifThenElse( (MY.DaskNProcs < 2)'
                              '          , MY.DaskNThreads'
                              '          , 1 + MY.DaskNProcs * MY.DaskNThreads)'

    , 'Periodic_Hold':
        '((time() - EnteredCurrentStatus) > MY.DaskWorkerTimeout) &&'
        ' (JobStatus == 2)'
    , 'Periodic_Hold_Reason': 'strcat("dask-worker exceeded max lifetime of ",'
                              '       interval(MY.DaskWorkerTimeout))'
    }

JOB_STATUS_IDLE = 1
JOB_STATUS_RUNNING = 2
JOB_STATUS_HELD = 5

SCRIPT_TEMPLATE = """\
#!/bin/bash

if [[ -z $_CONDOR_SCRATCH_DIR ]]; then
    echo '$_CONDOR_SCRATCH_DIR not defined'
    echo 'This script needs to be run as an HTCondor job'
    exit 1
fi

export HOME=$_CONDOR_SCRATCH_DIR

tar xzf ~/%(worker_tarball)s
export PATH=~/dask_condor_worker/bin:$PATH

args=( "$@" )

# This isn't actually necessary - $TMP is already under $_CONDOR_SCRATCH_DIR
# so that's where mktemp will make its files.
local_directory=$_CONDOR_SCRATCH_DIR/.worker
mkdir -p "$local_directory"
args+=(--local-directory "$local_directory")

exec python ~/dask_condor_worker/bin/dask-worker "${args[@]}"
"""


_global_schedulers = [] # (scheduler_id, schedd)


@atexit.register
def global_killall():
    for sid, schedd in _global_schedulers:
        condor_rm(schedd, 'DaskSchedulerId == "%s"' % sid)


def worker_constraint(jobid):
    if '.' in jobid:
        clusterid, procid = jobid.split('.', 1)
        return '(ClusterId == %s && ProcId == %s)' % (clusterid, procid)
    else:
        return '(ClusterId == %s)' % jobid


def or_constraints(constraints):
    return '(' + ' || '.join(constraints) + ')'


def workers_constraint(jobids):
    return or_constraints([worker_constraint(jid) for jid in jobids])


def condor_rm(schedd, job_spec):
    return schedd.act(htcondor.JobAction.Remove, job_spec)


class Error(Exception):
    pass


class HTCondorCluster(object):
    def __init__(self,
                 memory_per_worker=1024,
                 procs_per_worker=1,
                 pool=None,
                 schedd_name=None,
                 threads_per_worker=1,
                 update_interval=1000,
                 worker_timeout=(24 * 60 * 60),
                 scheduler_port=8786,
                 worker_tarball=None,
                 **kwargs):

        self.memory_per_worker = memory_per_worker
        self.procs_per_worker = procs_per_worker
        self.threads_per_worker = threads_per_worker
        if int(update_interval) < 1:
            raise ValueError("update_interval must be >= 1")
        self.worker_timeout = worker_timeout
        self.worker_tarball = worker_tarball

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

    def start_workers(self,
                      n=1,
                      memory_per_worker=None,
                      procs_per_worker=None,
                      threads_per_worker=None,
                      worker_timeout=None,
                      extra_attribs=None):
        n = int(n)
        if n < 1:
            raise ValueError("n must be >= 1")
        memory_per_worker = int(memory_per_worker or self.memory_per_worker)
        if memory_per_worker < 1:
            raise ValueError("memory_per_worker must be >= 1 (MB)")
        procs_per_worker = int(procs_per_worker or self.procs_per_worker)
        if procs_per_worker < 1:
            raise ValueError("procs_per_worker must be >= 1")
        threads_per_worker = int(threads_per_worker or self.threads_per_worker)
        if threads_per_worker < 1:
            raise ValueError("threads_per_worker must be >= 1")
        worker_timeout = int(worker_timeout or self.worker_timeout)
        if worker_timeout < 1:
            raise ValueError("worker_timeout must be >= 1 (sec)")

        job = htcondor.Submit(JOB_TEMPLATE)
        job['MY.DaskSchedulerAddress'] = '"' + self.scheduler_address + '"'
        job['MY.DaskNProcs'] = str(procs_per_worker)
        job['MY.DaskNThreads'] = str(threads_per_worker)
        job['RequestMemory'] = str(memory_per_worker)
        job['MY.DaskSchedulerId'] = '"' + self.scheduler.id + '"'
        job['MY.DaskWorkerTimeout'] = str(worker_timeout)
        if self.script:
            job['Executable'] = self.script.name
            job['Transfer_Input_Files'] = self.worker_tarball

        if extra_attribs:
            job.update(extra_attribs)

        classads = []
        with self.schedd.transaction() as txn:
            clusterid = job.queue(txn, count=n, ad_results=classads)
        logger.info("%d job(s) submitted to cluster %s." % (n, clusterid))
        for ad in classads:
            self.jobs["%s.%s" % (ad['ClusterId'], ad['ProcId'])] = ad

    def killall(self):
        condor_rm(self.schedd, self.scheduler_constraint)

    def submit_worker(self, **kwargs):
        return self.start_workers(n=1, **kwargs)

    def stop_workers(self, worker_ids):
        if isinstance(worker_ids, str):
            worker_ids = [worker_ids]

        constraint = '%s && %s' % (
            self.scheduler_constraint,
            workers_constraint(worker_ids)
            )

        condor_rm(self.schedd, constraint)

    def update_jobs(self):
        try:
            ads = self.schedd.xquery(
                    self.scheduler_constraint,
                    projection=['ClusterId', 'ProcId', 'JobStatus'])
            active_jobids = []
            for ad in ads:
                jobid = '%(ClusterId)s.%(ProcId)s' % (ad)
                jobstatus = ad['JobStatus']
                if jobstatus in (
                        JOB_STATUS_IDLE, JOB_STATUS_RUNNING, JOB_STATUS_HELD):
                    self.jobs[jobid]['JobStatus'] = jobstatus
                    active_jobids.append(jobid)

            # Evaluate the list of keys now to avoid a RuntimeError when
            # we delete items from the dict mid-iteration
            for jobid in list(self.jobs.keys()):
                if jobid not in active_jobids:
                    del self.jobs[jobid]
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
            for path in ['dask_condor_worker/bin/python',
                         'dask_condor_worker/bin/dask-worker']:
                if path not in members:
                    raise Error("Expected file %s not in tarball" % (path))
        finally:
            tar.close()


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

