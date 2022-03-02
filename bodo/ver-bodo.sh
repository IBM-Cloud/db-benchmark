#!/bin/bash
set -e

source ./pandas/py-pandas/bin/activate
python -c 'import bodo; open("bodo/VERSION","w").write(bodo.__version__); open("bodo/REVISION","w").write(bodo.__version__);' > /dev/null
