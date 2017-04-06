#!/usr/bin/env python
from __future__ import print_function

import logging
import sys
import time
import unittest

import distributed
import socket
sys.path.insert(0, '.')
import dask_condor


def sleepsort(arg):
    time.sleep(arg - ord('@'))
    print(arg)
    return arg


class TestDaskCondor(unittest.TestCase):
#    def setUp(self):
#        return
#
#    def tearDown(self):
#        return
#
    def test_dask_condor(self):
        cluster = dask_condor.HTCondorCluster(memory_per_worker=256,
                                              schedd_name=socket.gethostname(),
                                              diagnostics_port=None)
        cluster.start_workers(n=2)
        cluster.start_workers(n=2, memory_per_worker=128)
        client = distributed.Client(cluster)
        A = client.map(ord, 'HELLO')
        B = client.map(sleepsort, A, pure=False)
        C = client.map(chr, B)
        print(client.gather(C))
        logging.info(cluster.jobids)
        try:
            client.shutdown()
        except distributed.core.CommClosedError:
            pass


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
