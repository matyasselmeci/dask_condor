#!/usr/bin/env python

from setuptools import setup

with open("requirements.txt") as f:
    install_requires = f.read().strip().split("\n")

setup(
    name='dask_condor',
    version='0.0.20181214',
    description='HTCondor backend for Dask.Distributed',
    author='Matyas Selmeci',
    author_email='matyas@cs.wisc.edu',
    url='https://github.com/matyasselmeci/dask_condor',
    packages=['dask_condor'],
    install_requires=install_requires,
    license="Apache Software License",
)
