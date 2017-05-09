#!/usr/bin/env python

import h5py
import random


X = 500
Y = 500

f = h5py.File('/tmp/test-datafile.hdf5', 'w')
dset = f.create_dataset("default", (X, Y), dtype='float64')
for x in xrange(X):
    for y in xrange(Y):
        dset[x,y] = random.random()
f.close()
