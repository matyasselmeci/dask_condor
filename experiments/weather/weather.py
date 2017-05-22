#!/usr/bin/env python

# based on https://gist.github.com/jasoncpatton/e51debc6cd579e9ecb219afc72a78e67

import dask.dataframe as dd
import pandas as pd
import logging

logging.basicConfig(level=10)

from distributed import Client
from dask_condor import HTCondorCluster

#sites = ['MSN', 'MKE', 'GRB', 'EAU', 'LSE', 'LNR', 'VOK', 'JVL', 'MTW', 'CWA', 'OSH', 'RHI', 'CMY', 'AUW']
sites = ['MSN']
year_start = 2007
year_end = 2016
url = 'http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?station={0}&data=tmpc&data=dwpc&data=relh&data=drct&data=sknt&data=mslp&data=p01m&year1={1}&month1=1&day1=1&year2={2}&month2=12&day2=31&tz=Etc%2FUTC&format=comma&latlon=no&direct=no&report_type=1&report_type=2'
# format with (site, year_start, year_end)

csvfile = "{0}-{1}-to-{2}.csv"
site = sites[0]
# dask doesn't support the 'index' parameter so load in pandas instead
pdf = pd.read_csv(
    #url.format(site, year_start, year_end),
    csvfile.format(site, year_start, year_end),
    index_col = 1, # index on timestamp
    skipinitialspace = True, comment = '#', na_values = ['M'],
    parse_dates = True, infer_datetime_format = True,
)

ddf = dd.from_pandas(pdf, chunksize=128)

cluster = HTCondorCluster(worker_tarball='dask_condor_worker_base.SL6.tar.gz')
client = Client(cluster)
cluster.start_workers(n=1)

ddf = ddf.resample('1H', label='right').last()

print(ddf.head())
