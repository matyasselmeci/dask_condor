#!/bin/bash

yum install --enablerepo=osg-upcoming -y python-pip condor condor-python python-devel
pip install dask distributed bokeh

cat > /etc/condor/config.d/20-num_slots.config <<__EOF__
# Bump up the number of slots so we can test multiple workers
NUM_CPUS = 8
__EOF__
