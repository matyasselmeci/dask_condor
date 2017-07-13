import htcondor


JOB_STATUS_IDLE = 1
JOB_STATUS_RUNNING = 2
JOB_STATUS_HELD = 5
HOLD_REASON_PERIODIC_HOLD = 2

def worker_constraint(jobid):
    if '.' in jobid:
        return '(JobId == "%s")' % jobid
    else:
        return '(ClusterId == "%s")' % jobid


def or_constraints(constraints):
    return '(' + ' || '.join(constraints) + ')'


def workers_constraint(jobids):
    return or_constraints([worker_constraint(jid) for jid in jobids])


def workers_constraint_by_name(names):
    return or_constraints(['(DaskWorkerName =?= "%s")' % n for n in names])


def condor_rm(schedd, job_spec):
    return schedd.act(htcondor.JobAction.Remove, str(job_spec))


def condor_release(schedd, job_spec):
    return schedd.act(htcondor.JobAction.Release, str(job_spec))

