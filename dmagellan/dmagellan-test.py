#!/usr/bin/env python

import logging, sys, getopt

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


def usage():
    print("-h --help               :  this message")
    print("-l --local              :  use LocalCluster() (i.e. multiprocessing)")
    print("-s --sched              :  connect to scheduler on 127.0.0.1:8786")
    print("--block-tables-chunks=N :  number of chunks to use in ob.block_tables")
    print("--extract-feature-vecs-chunks=N :  number of chunks to use in extract_feature_vecs")
    print("--usecols               :  load a subset of the columns from the datafile")
    print("Need one of -s, -l")

try:
    opts, _ = getopt.gnu_getopt(
        sys.argv[1:], 'hls', ['block-tables-chunks=', 'help',  'local', 'sched', 'usecols'])
except getopt.GetoptError as err:
    print(str(err))
    usage()
    sys.exit(2)

client = None
usecols = None
block_tables_chunks = 4
extract_feature_vecs_chunks = 4
for o, a in opts:
    if o in ('-h', '--help'):
        usage()
        sys.exit()
    elif o == '--block-tables-chunks':
        block_tables_chunks = a
    elif o == '--extract-feature-vecs-chunks':
        extract_feature_vecs_chunks = a
    elif o in ('-l', '--local'):
        logging.info("Using LocalCluster")
        client = Client()
    elif o in ('-s', '--sched'):
        logging.info("Connecting to scheduler at 127.0.0.1:8786")
        client = Client('127.0.0.1:8786')
    elif o == '--usecols':
        usecols = ['id', 'title']
    else:
        assert False, 'unhandled option'
if not client:
    print("--local or --sched must be specified!")
    sys.exit(2)
logging.debug('got client')

logging.info("Parameters:")
logging.info("block_tables_chunks: %s" % block_tables_chunks)
logging.info("extract_feature_vecs_chunks: %s" % extract_feature_vecs_chunks)
logging.info("usecols: %s" % usecols)

orig_A = pd.read_csv('./data/citeseer_nonans.csv', usecols=usecols)
orig_B = pd.read_csv('./data/dblp_nonans.csv', usecols=usecols)
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
                    overlap_size=6, nltable_chunks=block_tables_chunks, nrtable_chunks=block_tables_chunks,
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
                          attrs_after='label', nchunks=extract_feature_vecs_chunks,
                          show_progress=False,
                          # we have to compute here else mlmatcher will
                          # complain that "Input table is not of type DataFrame"
                          compute=True,
                         scheduler=client.get)
logging.debug('finished H=extract_feature_vecs()')
logging.debug("len(H) = %d" % len(H))


sys.exit(0)  # EXIT ------------------------


# Instantiate the matcher to evaluate.
dt = DTMatcher(name='DecisionTree', random_state=0)

dt.fit(table=H, 
       exclude_attrs=['_id', 'l_id', 'r_id', 'label'], 
       target_attr='label')
logging.debug('ran dt.fit()')

# Convert J into a set of feature vectors using F
I = extract_feature_vecs(C, A, B,
                         '_id', 'l_id', 'r_id', 'id', 'id',
                            nchunks=extract_feature_vecs_chunks,
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

