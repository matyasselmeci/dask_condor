#!/bin/bash

# Build Python, install the dependencies for dask, and package it up in a
# tarball.

set -eux

export HOME=${_CONDOR_SCRATCH_DIR?'$_CONDOR_SCRATCH_DIR not defined; this script must be run under Condor'}

if [[ $# -ne 2 ]]; then
    echo "Usage: $(basename $0) <source_archive> <target_archive>"
    exit 2
fi

python_source_archive=~/$1
python_target_archive=~/$2
python_build_dir=${python_source_archive%.tar.xz}
python_install_dir=${python_build_dir}/../python
python_bin_dir=${python_install_dir}/bin
requirements_file=~/requirements.txt


hostname -f || :
env | sort || :


# If the SSL headers aren't installed, Python will silently fail to build the
# `_ssl` module, causing pip to fail because it can't use HTTPS.
echo '*** Checking requirements'
if [[ ! -f /usr/include/openssl/ssl.h ]]; then
    echo '*** OpenSSL development headers not found'
    echo 'On an RPM system, these should be in the "openssl-devel" package'
    exit 1
fi

if [[ ! -f $python_source_archive ]]; then
    echo "*** Python source archive not found at $python_source_archive"
    exit 1
fi


cd
xzcat $python_source_archive | tar x
cd $python_build_dir

if ! ./configure --prefix=${python_install_dir} --with-ensurepip=install; then
    echo "*** configure failed. config.log follows:"
    cat config.log
    exit 1
fi
make
make install

if [[ ! -x $python_bin_dir/python ]]; then
    # We don't have an actual executable named `python`. We may have one named
    # python2 or python3; in that case, make a copy
    cp "$python_bin_dir"/python[2-9] "$python_bin_dir/python"
fi

export PATH=$python_bin_dir:$PATH

echo '*** Running Python test program'
python - <<__end__
import sys
from distutils import sysconfig
sys.stdout.write(sysconfig.PREFIX + '\n')

import ssl
sys.stdout.write('ok\n')
__end__

echo '*** Downloading and installing dask.distributed and requirements'
pip install -r "$requirements_file"

echo '*** Testing that dask.distributed got properly installed'
python - <<__end__
import dask, dask.array, distributed, sys
sys.stdout.write('ok\n')
__end__
which dask-worker

echo '*** Tarring up results'
cd
tar czf "$python_target_archive" "${python_install_dir}"
