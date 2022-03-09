#!/usr/bin/bash

if [ "$1" = "" ]
then
  echo "You must provide path to csv file as parameter"
  exit
fi

filename=$1
file_basename="${filename%.*}"
partitioned_dir="${filename%.*}"_partitioned

echo "Partitioning $1 into parquet files in folder $partitioned_dir"

mkdir $partitioned_dir

echo "Splitting CSV file into smaller files..."

split -d -l 62500000 $filename $partitioned_dir/part- --additional-suffix=.csv

echo "Adding headers to each split CSV file..."

for i in {01..15}; do file=$partitioned_dir/part-$i.csv; { head -1 $filename; cat $file; } >$file.new ; mv $file{.new,}; done

cat $partitioned_dir/part-16.csv >> $partitioned_dir/part-15.csv

echo "Converting each split CSV file to parquet..."

for i in {00..15}; do python -c "import pandas as pd; part='$i'; pd.read_csv('$partitioned_dir/part-'+part+'.csv').to_parquet('$partitioned_dir/part-'+part+'.parquet')"; done

echo "Cleaning up split CSV files..."

rm $partitioned_dir/*.csv

echo "Done"
