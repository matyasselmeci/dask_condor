import logging
import distributed
import socket
import sys
from unittest import TestCase

if '.' not in sys.path:
    sys.path.insert(0, '.')

from dask_condor import HTCondorCluster


class HTCondorClusterTestCase(TestCase):
    def _print_memory_usage(self):
        for jobid in sorted(self.cluster.jobids):
            logfile = "worker-%s.log" % jobid
            with open(logfile) as logfh:
                memusagelines = [line.strip().split()[0] for line in logfh
                                 if 'MemoryUsage' in line]
                if memusagelines:
                    logging.debug("Job %5s: MemoryUsage %5s" % (
                                   jobid, memusagelines[-1]))

    def setUp(self):
        self.cluster = HTCondorCluster(memory_per_worker=128,
                                       schedd_name=socket.gethostname(),
                                       diagnostics_port=None)
        self.client = distributed.Client(self.cluster)

    def tearDown(self):
        try:
            self._print_memory_usage()
        except Exception as err:
            logging.exception(err)
        try:
            self.client.shutdown()
        except distributed.core.CommClosedError:
            pass
