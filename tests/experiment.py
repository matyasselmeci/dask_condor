#!/usr/bin/env python
import logging
import sys
import unittest

sys.path.insert(0, '.')

from dask_condor.tests import HTCondorClusterTestCase
from dask_condor import HTCondorCluster


class RunExperiments(HTCondorClusterTestCase):
    def test_memory_usage(self):
        # Run workers with a different number of processes.
        # The base class will print the memory usage (as reported by condor)
        # after the test.
        for i in range(1,9):
            self.cluster.submit_worker(procs_per_worker=i,
                                       # lie about how many CPUs we want so we
                                       # get scheduled
                                       extra_attribs={'RequestCpus': '1'})
        data = list(range(1,9))
        future = self.client.map(lambda x: x - 1, data)
        self.assertEqual(range(8), self.client.gather(future))


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('distributed.comm.tcp').setLevel(logging.ERROR)
    logging.getLogger('tornado.application').setLevel(logging.CRITICAL)
    unittest.main(verbosity=2)
