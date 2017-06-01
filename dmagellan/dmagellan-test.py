#!/usr/bin/env python

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

orig_A = pd.read_csv('./data/citeseer_nonans.csv')
orig_B = pd.read_csv('./data/dblp_nonans.csv')

# sample datasets
A = pd.read_csv('./data/sample_citeseer.csv')
B = pd.read_csv('./data/sample_dblp.csv')

# blocking
ob = OverlapBlocker()
C = ob.block_tables(A, B, 'id', 'id', 'title', 'title', 
                    overlap_size=3, nltable_chunks=2, nrtable_chunks=2, 
                    scheduler=client.get, compute=False, 
                    rem_stop_words=True
                   )

L = pd.read_csv('./data/sample_labeled_data.csv')

F = em.get_features_for_matching(A, B)

# Convert L into feature vectors using updated F
H = extract_feature_vecs(L, orig_A, orig_B, 
                         '_id', 'l_id', 'r_id', 'id', 'id', 
                          feature_table=F, 
                          attrs_after='label', nchunks=4,
                          show_progress=True, compute=True, 
                         scheduler=client.get)


print(H.head())

# Instantiate the matcher to evaluate.
dt = DTMatcher(name='DecisionTree', random_state=0)

dt.fit(table=H, 
       exclude_attrs=['_id', 'l_id', 'r_id', 'label'], 
       target_attr='label')

# Convert J into a set of feature vectors using F
I = extract_feature_vecs(C, A, B,
                         '_id', 'l_id', 'r_id', 'id', 'id', 
                            nchunks=4,
                            feature_table=F,
                            show_progress=True,
                            compute=False)


predictions = dt.predict(table=I, exclude_attrs=['_id', 'l_id', 'r_id'], 
              append=True, target_attr='predicted', inplace=False,
                        nchunks=4, scheduler=client.get, compute=False)

predictions.visualize()

p = predictions.compute(get=client.get)
print(p)
