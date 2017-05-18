#!/bin/bash

# Build Python, install the dependencies for dask, and package it up in a
# tarball.

set -eux


realpath () {
    python -c "import os, sys; sys.stdout.write(os.path.realpath(r'''$1''') + '\n')"
}


work_dir=$(mktemp -dt dask_condor_worker.XXXXXX)
trap 'rm -rf "$work_dir"' EXIT

if [[ $# -ne 3 ]]; then
    echo "Usage: $(basename $0) <source_archive> <target_archive> <requirements_file>"
    exit 2
fi

python_source_archive=$(realpath "$1")
python_target_archive=$(realpath "$2")
requirements_file=$(realpath "$3")
python_build_dir=$work_dir/$(basename "$python_source_archive" .tar.xz)
python_install_dir=$work_dir/dask_condor_worker
python_bin_dir=$python_install_dir/bin


# DEBUGGING
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


cd "$work_dir"
xzcat "$python_source_archive" | tar x
cd "$python_build_dir"

if ! ./configure --prefix="$python_install_dir" --with-ensurepip=install; then
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

PATH=$python_bin_dir:$PATH
export PATH

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
cd "$work_dir"
tar czf "$python_target_archive" "$(basename "$python_install_dir")"
