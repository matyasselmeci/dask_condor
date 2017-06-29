#!/usr/bin/env python

import logging, sys, argparse, os

logging.basicConfig(level=0, format="%(asctime)-15s %(name)s %(message)s")


# from https://github.com/kvpradap/dmagellan/blob/chtc/notebooks/chtc/example.ipynb

import pandas as pd

# imports
from dmagellan.blocker.attrequivalence.attr_equiv_blocker import AttrEquivalenceBlocker
from dmagellan.blocker.overlap.overlapblocker import OverlapBlocker
from dmagellan.blocker.blackbox.blackbox_blocker import BlackBoxBlocker
from dmagellan.blocker.rulebased.rule_based_blocker import RuleBasedBlocker
from dmagellan.feature.autofeaturegen import get_features_for_blocking
from dmagellan.feature.extractfeatures import extract_feature_vecs
from dmagellan.feature.autofeaturegen import get_features_for_matching
from dmagellan.matcher.dtmatcher import DTMatcher
from dmagellan.utils.py_utils.utils import concat_df
from dask import delayed
from dask.threaded import get
from distributed import Client
import py_entitymatching as em


parser = argparse.ArgumentParser(description="dmagellan test")
parser.add_argument('client_type', choices=['local', 'sched', 'condor'],
                    help='use LocalCluster, a scheduler on 127.0.0.1:8786, or '
                    'an HTCondorCluster')
parser.add_argument('--block-tables-chunks', metavar='N', type=int, default=4,
                    help='number of chunks to use in ob.block_tables')
parser.add_argument('--extract-feature-vecs-chunks', metavar='N', type=int, default=4,
                    help='number of chunks to use in extract_feature_vecs')
parser.add_argument('--usecols', action='store_const', const=['id', 'title'], default=None,
                    help='load a subset of the columns from the datafile')
parser.add_argument('--workers', type=int, default=16,
                    help='Number of workers to start ("condor" client only)')
args = parser.parse_args()

if args.client_type == 'local':
    logging.info("Using LocalCluster")
    client = Client()
elif args.client_type == 'sched':
    logging.info("Connecting to scheduler at 127.0.0.1:8786")
    client = Client('127.0.0.1:8786')
elif args.client_type == 'condor':
    logging.info("Creating HTCondorCluster")
    from dask_condor import HTCondorCluster

    worker_tarball="dask_condor_worker_dmagellan.854e51d.SL6.tar.gz"
    if os.path.exists(os.path.join('/squid/matyas', worker_tarball)):
        worker_tarball = "http://proxy.chtc.wisc.edu/SQUID/matyas/" + worker_tarball
    elif not os.path.exists(worker_tarball):
        worker_tarball = "http://research.cs.wisc.edu/~matyas/dask_condor/" + worker_tarball

    htc = HTCondorCluster(memory_per_worker=4096,
                          update_interval=10000,
                          worker_tarball=worker_tarball,
                          logdir=".log")
    htc.start_workers(n=args.workers)

    client = Client(htc)
else:
    assert False, 'should have been caught'

logging.info("Parameters:")
logging.info("block_tables_chunks: %s" % args.block_tables_chunks)
logging.info("extract_feature_vecs_chunks: %s" % args.extract_feature_vecs_chunks)
logging.info("usecols: %s" % args.usecols)

orig_A = pd.read_csv('./data/citeseer_nonans.csv', usecols=args.usecols)
orig_B = pd.read_csv('./data/dblp_nonans.csv', usecols=args.usecols)
logging.debug('loaded full data')

# sample datasets
A = pd.read_csv('./data/sample_citeseer.csv')
B = pd.read_csv('./data/sample_dblp.csv')
logging.debug('loaded sample data')

# blocking
ob = OverlapBlocker()
logging.debug("starting ob.block_tables()")
C = ob.block_tables(orig_A, orig_B, 'id', 'id', 'title', 'title',
                    # increasing the chunks bloats the memory usage of the client
                    # and also how long it takes before it creates tasks
                    # but it's not a problem if I set usecols when loading orig_A
                    # and orig_B, or if I use lz4 compression
                    overlap_size=6, nltable_chunks=args.block_tables_chunks, nrtable_chunks=args.block_tables_chunks,
                    # I set compute to True to see which part of the workflow was
                    # taking a long time
                    scheduler=client.get, compute=True,
                    rem_stop_words=True
                   )
logging.debug('finished ob.block_tables()')
logging.debug('len(C) = %d' % len(C))

L = pd.read_csv('./data/sample_labeled_data.csv')
logging.debug('loaded sample labeled data')

F = em.get_features_for_matching(orig_A, orig_B)
logging.debug('ran em.get_features_for_matching()')

# Convert L into feature vectors using updated F
# must use orig_A and orig_B; I get a KeyError when I try to use the sample data
# requires workers with > 1GB of memory, otherwise (sometimes) does not finish
logging.debug("starting H=extract_feature_vecs()")
H = extract_feature_vecs(L, orig_A, orig_B,
                         '_id', 'l_id', 'r_id', 'id', 'id',
                          feature_table=F,
                    # increasing the chunks bloats the memory usage of the client
                    # and also how long it takes before it creates tasks
                          attrs_after='label', nchunks=args.extract_feature_vecs_chunks,
                          show_progress=False,
                          # we have to compute here else mlmatcher will
                          # complain that "Input table is not of type DataFrame"
                          compute=True,
                         scheduler=client.get)
logging.debug('finished H=extract_feature_vecs()')
logging.debug("len(H) = %d" % len(H))



# Instantiate the matcher to evaluate.
dt = DTMatcher(name='DecisionTree', random_state=0)

dt.fit(table=H, 
       exclude_attrs=['_id', 'l_id', 'r_id', 'label'], 
       target_attr='label')
logging.debug('ran dt.fit()')

# Convert J into a set of feature vectors using F
I = extract_feature_vecs(C, orig_A, orig_B,
                         '_id', 'l_id', 'r_id', 'id', 'id',
                            nchunks=args.extract_feature_vecs_chunks,
                            feature_table=F,
                            show_progress=False,
                            compute=False)
logging.debug('ran I=extract_feature_vecs()')


predictions = dt.predict(table=I, exclude_attrs=['_id', 'l_id', 'r_id'], 
              append=True, target_attr='predicted', inplace=False,
                        nchunks=4, scheduler=client.get, compute=False)
logging.debug('ran dt.predict()')

# Can't visualize - no graphviz
#predictions.visualize()

p = predictions.compute(get=client.get)
logging.debug('ran predictions.compute()')
print(p)
logging.debug('done')

