#!/usr/bin/env python
from __future__ import print_function

import os
import time

import htcondor
import classad



def submit(schedd, job, count=1):
    with schedd.transaction() as txn:
        return job.queue(txn, count=count, ad_results=None)


def submit_with_results(schedd, job, count=1):
    adslist = []
    with schedd.transaction() as txn:
        clusterid = job.queue(txn, count=count, ad_results=adslist)
        return (clusterid, adslist)


def remove_cluster(schedd, clusterid):
    return schedd.act(htcondor.JobAction.Remove, "ClusterId == %d" % clusterid)

sleepjob = htcondor.Submit(dict(
    Executable="/usr/bin/sleep",
    Arguments="300",
    Universe="vanilla",
    ))


schedd = htcondor.Schedd()

clusterid = submit(schedd, sleepjob, count=4)

print("submitted %s" % clusterid)

os.system("condor_q")

time.sleep(5)

remove_cluster(schedd, clusterid)

time.sleep(5)

os.system("condor_q")
os.system("condor_history %d" % clusterid)

