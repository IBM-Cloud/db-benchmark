#!/bin/bash
set -e

# install all dependencies
sudo apt-get update
sudo apt-get install build-essential python3-dev python3-pip

virtualenv bodo/py-bodo --python=/usr/bin/python3.9
source bodo/py-bodo/bin/activate

# install binaries
python -m pip install --upgrade psutil
python -m pip install --upgrade bodo

# install datatable for fast data import
python -m pip install --upgrade datatable

# check
python
import bodo
bodo.__version__
quit()
deactivate
