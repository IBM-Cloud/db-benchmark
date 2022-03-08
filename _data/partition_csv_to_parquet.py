import bodo
import pandas as pd
import sys
import os

@bodo.jit
def split(input_file, output_dir):
    df = pd.read_csv(input_file)
    df.to_parquet(output_dir)

split(sys.argv[1], os.path.splitext(sys.argv[1])[0] + "_partitioned")

