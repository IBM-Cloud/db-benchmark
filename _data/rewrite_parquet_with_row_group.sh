#!/usr/bin/bash

if [ "$1" = "" ]
then
  echo "You must provide path to partitioned parquet directory as parameter"
  exit
fi

partitioned_dir="$1"

mkdir "$partitioned_dir""_new"

echo "Rewriting files in $partitioned_dir with row_group_size=1000000 into $partitioned_dir""_new ..."

for i in {00..15}; do python -c "import pandas as pd; part='$i'; partitioned_dir='$partitioned_dir'; pd.read_parquet(partitioned_dir+'/part-'+part+'.parquet').to_parquet(partitioned_dir+'_new/part-'+part+'.parquet', row_group_size=1_000_000)"; done

echo "Done"
