#!/usr/bin/env python

import dask, distributed
import logging
import pprint

def probe(dask_worker):
    return dict(fastdata=len(dask_worker.data.fast),
                slowdata=len(dask_worker.data.slow),
                kbytesused=sum(dask_worker.nbytes.values()) // 1024)

logging.getLogger('distributed.comm.tcp').setLevel(logging.ERROR)

cli = distributed.Client(scheduler_file='/tmp/schedfile')

pprint.pprint(cli.run(probe))

nbytes = cli.nbytes()
nkbytes = dict()
for key in nbytes:
    nkbytes[key + ' (kb)'] = nbytes[key] // 1024

pprint.pprint(nkbytes)

