#!/usr/bin/env python
from __future__ import print_function

# based on https://gist.github.com/jasoncpatton/e51debc6cd579e9ecb219afc72a78e67

import dask.dataframe as dd
import pandas as pd
import logging
import time

logging.basicConfig(level=10)

from distributed import Client
from dask_condor import HTCondorCluster
from distributed.deploy.adaptive import Adaptive

sites = ['MSN', 'MKE', 'GRB', 'EAU', 'LSE', 'LNR', 'VOK', 'JVL', 'MTW', 'CWA', 'OSH', 'RHI', 'CMY', 'AUW']
#sites = ['MSN']
year_start = 2007
year_end = 2016


cluster = HTCondorCluster(worker_tarball='dask_condor_worker_base.SL6.tar.gz')
client = Client(cluster)
adaptive = Adaptive(cluster.scheduler, cluster, interval=20000)

csvfile = "{0}-{1}-to-{2}.csv"
ddfs = {}
for site in sites:
    print("Scheduling site: " + site)
    # dask doesn't support the 'index' parameter so load in pandas instead
    pdf = pd.read_csv(
        #url.format(site, year_start, year_end),
        csvfile.format(site, year_start, year_end),
        index_col = 1, # index on timestamp
        skipinitialspace = True, comment = '#', na_values = ['M'],
        parse_dates = True, infer_datetime_format = True,
    )
    ddf = dd.from_pandas(pdf, chunksize=128)
    ddf = ddf.resample('1H', label='right').last()
    ddfs[site] = ddf

for site, ddf in ddfs.items():
    print("Calculating site {0} with cluster {1!s}:".format(site, cluster))
    st = time.time()
    # compute the whole data frame (I think head might only compute the first n lines?)
    ddf.compute()
    print(ddf.head())
    print("{0:.1f}s".format(time.time() - st))
    print()
