#!/usr/bin/env python
from __future__ import print_function

import logging
import os
import sys
import unittest

import dask.array
import distributed


if __name__ == "__main__" and __package__ is None:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests import HTCondorClusterTestCase
from dask_condor import HTCondorCluster


class TestDaskCondor(HTCondorClusterTestCase):
    def test_array(self):
        self.cluster.start_workers(n=4)
        x = dask.array.ones((500, 500), chunks=(10, 10))
        future = self.client.compute(x.sum())
        result = int(self.client.gather(future))
        self.assertEqual(result, 500**2)

    def test_simple(self):
        self.cluster.start_workers(n=4)
        future = self.client.map(chr, [72, 69, 76, 76, 79, 32, 67, 79, 78, 68, 79, 82])
        self.assertEqual(''.join(self.client.gather(future)), 'HELLO CONDOR')


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('distributed.comm.tcp').setLevel(logging.ERROR)
    logging.getLogger('tornado.application').setLevel(logging.CRITICAL)
    unittest.main(verbosity=2)
