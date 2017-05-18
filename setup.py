#!/usr/bin/env python

from setuptools import setup

setup(
    name='dask_condor',
    version='0.0.20170518',
    description='HTCondor backend for Dask.Distributed',
    author='Matyas Selmeci',
    author_email='matyas@cs.wisc.edu',
    url='https://github.com/matyasselmeci/dask_condor',
    packages=['dask_condor'],
    install_requires=['dask',
                      'distributed',
                      'bokeh'],
    license="Apache Software License",
)
