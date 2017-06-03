#!/usr/bin/env python

import logging

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

client = Client('127.0.0.1:8786')
logging.debug('got client')

orig_A = pd.read_csv('./data/citeseer_nonans.csv')
orig_B = pd.read_csv('./data/dblp_nonans.csv')
logging.debug('loaded full data')

# sample datasets
A = pd.read_csv('./data/sample_citeseer.csv')
B = pd.read_csv('./data/sample_dblp.csv')
logging.debug('loaded sample data')

# blocking
ob = OverlapBlocker()
# can't run this with the full data - I get a "Failed to serialize" error
# with the specifics "error: 'i' format requires -2147483648 <= number <= 2147483647"
C = ob.block_tables(A, B, 'id', 'id', 'title', 'title',
                    # increasing the chunks bloats the memory usage of the client
                    # and also how long it takes before it creates tasks
                    overlap_size=3, nltable_chunks=2, nrtable_chunks=2,
                    # I set compute to True to see which part of the workflow was
                    # taking a long time
                    scheduler=client.get, compute=True,
                    rem_stop_words=True
                   )
logging.debug('ran ob.block_tables()')

L = pd.read_csv('./data/sample_labeled_data.csv')
logging.debug('loaded sample labeled data')

F = em.get_features_for_matching(A, B)
logging.debug('ran em.get_features_for_matching()')

# Convert L into feature vectors using updated F
# must use orig_A and orig_B; I get a KeyError when I try to use the sample data
# requires workers with > 1GB of memory, otherwise (sometimes) does not finish
H = extract_feature_vecs(L, orig_A, orig_B,
                         '_id', 'l_id', 'r_id', 'id', 'id',
                          feature_table=F,
                    # increasing the chunks bloats the memory usage of the client
                    # and also how long it takes before it creates tasks
                          attrs_after='label', nchunks=4,
                          show_progress=True,
                          # we have to compute here else mlmatcher will
                          # complain that "Input table is not of type DataFrame"
                          compute=True,
                         scheduler=client.get)
logging.debug('ran H=extract_feature_vecs()')


print(H.head())

# Instantiate the matcher to evaluate.
dt = DTMatcher(name='DecisionTree', random_state=0)

dt.fit(table=H, 
       exclude_attrs=['_id', 'l_id', 'r_id', 'label'], 
       target_attr='label')
logging.debug('ran dt.fit()')

# Convert J into a set of feature vectors using F
I = extract_feature_vecs(C, A, B,
                         '_id', 'l_id', 'r_id', 'id', 'id',
                            nchunks=4,
                            feature_table=F,
                            show_progress=True,
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

