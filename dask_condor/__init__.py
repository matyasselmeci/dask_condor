"""
Make dask workers using condor
"""
from __future__ import division, print_function

import atexit
import logging
import time

import distributed
import tornado

import htcondor
import classad

if not hasattr(htcondor, 'Submit'):
    raise ImportError("htcondor.Submit not found; HTCondor 8.6.0 or newer required")


logger = logging.getLogger(__name__)

JOB_TEMPLATE = dict(
    Executable="/usr/bin/dask-worker",
    Universe="vanilla",
    Output="worker-$(ClusterId).$(ProcId).out",
    Error="worker-$(ClusterId).$(ProcId).err",
    Log="worker-$(ClusterId).$(ProcId).log",
)

JOB_STATUS_IDLE = 1
JOB_STATUS_RUNNING = 2
JOB_STATUS_HELD = 5

_global_schedulers = [] # (scheduler_id, schedd)

@atexit.register
def global_killall():
    for sid, schedd in _global_schedulers:
        condor_rm(schedd, 'DaskSchedulerId == "%s"' % sid)


def worker_constraint(jobid):
    clusterid, procid = jobid.split('.', 1)
    return '(ClusterId == %s && ProcId == %s)' % (clusterid, procid)


def or_constraints(constraints):
    return '(' + ' || '.join(constraints) + ')'


def workers_constraint(jobids):
    return or_constraints([worker_constraint(jid) for jid in jobids])


def condor_rm(schedd, job_spec):
    return schedd.act(htcondor.JobAction.Remove, job_spec)


class HTCondorCluster(object):
    def __init__(self,
                 memory_per_worker=1024,
                 procs_per_worker=1,
                 pool=None,
                 schedd_name=None,
                 threads_per_worker=1,
                 cleanup_interval=1000,
                 worker_timeout=(24 * 60 * 60),
                 **kwargs):

        global _global_schedulers

        if schedd_name is None:
            self.schedd = htcondor.Schedd()
        else:
            collector = htcondor.Collector(pool)
            self.schedd = htcondor.Schedd(
                collector.locate(
                    htcondor.DaemonTypes.Schedd,
                    schedd_name))

        self.local_cluster = distributed.LocalCluster(ip='', n_workers=0,
                                                      **kwargs)

        _global_schedulers.append((self.scheduler.id, self.schedd))

        self.jobs = {}  # {jobid: CLASSAD}
        if int(cleanup_interval) < 1:
            raise ValueError("cleanup_interval must be >= 1")
        self._cleanup_callback = tornado.ioloop.PeriodicCallback(
            callback=self.cleanup_jobs,
            callback_time=cleanup_interval,
            io_loop=self.scheduler.loop)
        self._cleanup_callback.start()

        self.memory_per_worker = memory_per_worker
        self.procs_per_worker = procs_per_worker
        self.threads_per_worker = threads_per_worker
        self.worker_timeout = worker_timeout

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
        args = [self.scheduler_address]
        args.append('--nprocs %d' % procs_per_worker)
        args.append('--nthreads %d' % threads_per_worker)
        # default memory limit is 75% of memory; use 75% of RequestMemory
        # instead
        args.append('--memory-limit %d' %
                    (memory_per_worker * 1048576 * 3 // 4))
        request_cpus = procs_per_worker * threads_per_worker
        if procs_per_worker > 1:
            # the nanny takes up a core too
            request_cpus += 1
        else:
            args.append('--no-nanny')
            # can only use --name if --nprocs=1
            worker_name = "htcondor-$(ClusterId).$(ProcId)"
            args.append('--name=' + worker_name)
            # when I tried +DaskWorkerName, then $(ClusterId) and $(ProcId) didn't
            # get expanded (GT #6219)
            job['MY.DaskWorkerName'] = '"' + worker_name + '"'

        args.append('--no-bokeh')

        job['Arguments'] = ' '.join(args)
        job['RequestMemory'] = "%d MB" % memory_per_worker
        job['RequestCpus'] = str(request_cpus)
        job['+DaskSchedulerId'] = '"' + self.scheduler.id + '"'

        job['Periodic_Hold'] = "((time() - EnteredCurrentStatus) > %d) &&" \
                               " (JobStatus == %d)" % (worker_timeout,
                                                       JOB_STATUS_RUNNING)
        job['Periodic_Hold_Reason'] = \
            '"dask-worker exceeded max lifetime of %d min"' % (
            worker_timeout // 60)

        if extra_attribs:
            job.update(extra_attribs)

        classads = []
        with self.schedd.transaction() as txn:
            clusterid = job.queue(txn, count=n, ad_results=classads)
        logger.info("Started clusterid %s with %d jobs" % (clusterid, n))
        logger.debug(
            "RequestMemory = %s; RequestCpus = %s"
            % (job['RequestMemory'], job['RequestCpus']))
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

    def cleanup_jobs(self):
        active_jobids = \
            ['%s.%s' % (ad['ClusterId'], ad['ProcId'])
             for ad in self.schedd.xquery(
                self.scheduler_constraint,
                projection=['ClusterId', 'ProcId', 'JobStatus'])
             if ad['JobStatus'] in (
                JOB_STATUS_IDLE,
                JOB_STATUS_RUNNING,
                JOB_STATUS_HELD)]
        for jobid in self.jobids:
            if jobid not in active_jobids:
                del self.jobs[jobid]

    def close(self):
        self.killall()
        self.local_cluster.close()

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __str__(self):
        return "<%s: %d workers>" % (self.__class__.__name__, len(self.jobids))

    __repr__ = __str__

