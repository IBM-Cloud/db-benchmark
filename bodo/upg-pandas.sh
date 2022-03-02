#!/bin/bash
set -e

echo 'upgrading bodo...'

source ./bodo/py-bodo/bin/activate

python -m pip install --upgrade bodo > /dev/null
