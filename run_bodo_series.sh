#!/usr/bin/bash
for i in {1..32}; do
   export BODO_CORES=$i
   echo "##########################################"
   echo "Running groupby with bodo using $i cores"
   ./_launcher/solution.R --solution=bodo --task=join --nrow=1e8
   echo ""
done
