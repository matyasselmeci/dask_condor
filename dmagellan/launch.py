# run this with python -i
import dask, distributed
from distributed import Client
from distributed.deploy.adaptive import Adaptive
from dask_condor import HTCondorCluster
import logging, os

logging.basicConfig(level=0)
logging.getLogger("distributed.comm.tcp").setLevel(logging.ERROR)
logging.getLogger("distributed.deploy.adaptive").setLevel(logging.WARNING)

worker_tarball="dask_condor_worker_dmagellan_conda.SL6.tar.gz"

if os.path.exists(os.path.join('/squid/matyas', worker_tarball)):
    worker_tarball = "http://proxy.chtc.wisc.edu/SQUID/matyas/" + worker_tarball
elif not os.path.exists(worker_tarball):
    worker_tarball = "http://research.cs.wisc.edu/~matyas/dask_condor/" + worker_tarball

htc = HTCondorCluster(disk_per_worker=4096*1024, memory_per_worker=4096, update_interval=10000, worker_tarball=worker_tarball, logdir=".log")
cli = Client(htc)
sch = htc.scheduler
print("htc={0}\ncli={1}\nsch={2}".format(htc,cli,sch))
