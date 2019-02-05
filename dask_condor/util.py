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


def _double_quotes(instr):
    return instr.replace("'", "''").replace('"', '""')


def quote_arguments(args):
    """Quote a string or list of strings using the Condor submit file "new" argument quoting rules.

    Returns a string.  Note that you cannot build an argument list by
    concatenating the results of multiple calls of this function; you must
    call this function with all the arguments you want to pass.

    Examples
    --------
    >>> quote_arguments(["3", "simple", "arguments"])
    '"3 simple arguments"'
    >>> quote_arguments(["one", "two with spaces", "three"])
    '"one \'two with spaces\' three"'
    >>> quote_arguments(["one", "\"two\"", "spacy 'quoted' argument"])
    '"one ""two"" \'spacey \'\'quoted\'\' argument\'"'
    """
    if isinstance(args, str):
        args_list = [args]
    else:
        args_list = args

    quoted_args = []
    for a in args_list:
        qa = _double_quotes(a)
        if ' ' in qa or "'" in qa:
            qa = "'" + qa + "'"
        quoted_args.append(qa)
    return '"' + " ".join(quoted_args) + '"'


def quote_environment(env):
    """Quote a dict of strings using the Condor submit file "new" environment quoting rules.

    Returns a string.  Note that you cannot build an environment list by
    concatenating the results of multiple calls of this function; you must
    call this function with all the arguments you want to pass.

    Examples:
    >>> quote_environment(OrderedDict([("one", 1), ("two", '"2"'), ("three", "spacey 'quoted' value")]))
    '"one=1 two=""2"" three=\'spacey \'\'quoted\'\' value\'"'
    """
    if not isinstance(env, dict):
        raise TypeError("env must be a dict")

    entries = []
    for k, v in env.items():
        qv = _double_quotes(str(v))
        if ' ' in qv or "'" in qv:
            qv = "'" + qv + "'"
        entries.append("%s=%s" % (k, qv))

    return '"' + " ".join(entries) + '"'

