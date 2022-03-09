#!/usr/bin/env python

import os
import gc
import timeit
import pandas as pd
import bodo
import time

exec(open("./_helpers/helpers.py").read())

ver = pd.__version__
git = pd.__git_version__
task = "join"
solution = "bodo"
fun = ".merge"
cache = "TRUE"
on_disk = "FALSE"

data_name = os.environ['SRC_DATANAME']
#src_jn_x = os.path.join(os.getcwd(), "data", data_name+".csv")
src_jn_x = os.path.join(os.getcwd(), "data", data_name+"_partitioned/")
y_data_name = join_to_tbls(data_name)
src_jn_small = os.path.join(os.getcwd(), "data", y_data_name[0]+".csv")
src_jn_medium = os.path.join(os.getcwd(), "data", y_data_name[1]+".csv")
#src_jn_big = os.path.join(os.getcwd(), "data", y_data_name[2]+".csv")
src_jn_big = os.path.join(os.getcwd(), "data", y_data_name[2]+"_partitioned/")

if(bodo.get_rank()==0):
  print("loading datasets " + src_jn_x + ", " + src_jn_small + ", " + src_jn_medium + ", " + src_jn_big, flush=True)

@bodo.jit
def rquestion(x,question,join_df,run,columns,join_type,ans_columns1,ans_columns2):
  t_start = time.time()
  ans = x.merge(join_df, how=join_type, on=columns)
  t = time.time() - t_start
  with bodo.objmode(m='float64'):
    m = memory_usage()
  t_start = time.time()
  chk = [ans[ans_columns1].sum(), ans[ans_columns2].sum()]
  chkt = time.time() - t_start
  with bodo.objmode:
    if(bodo.get_rank()==0):
      write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=run, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)

@bodo.jit(cache=True)
def run(src_jn_x, src_jn_small, src_jn_medium, src_jn_big):
  print("Starting to read base dataframe")
  task_init = time.time()
  #x = pd.read_csv(src_jn_x)
  x = pd.read_parquet(src_jn_x)
  print(f"done reading base dataframe in {time.time()-task_init}")

  task_init = time.time()
  print("Starting to read small join dataframe")
  small = pd.read_csv(src_jn_small)
  print(f"done reading small join dataframe in {time.time()-task_init}")
  print("Starting to read medium join dataframe")
  task_init = time.time()
  medium = pd.read_csv(src_jn_medium)
  print(f"done reading medium join dataframe in {time.time()-task_init}")
  print("Starting to read big join dataframe")
  task_init = time.time()
  #big = pd.read_csv(src_jn_big)
  big = pd.read_parquet(src_jn_big)
  print(f"done reading big join dataframe in {time.time()-task_init}")

  task_init = time.time()
  print("joining...")

  rquestion(x=x,question="small inner on int",join_df=small,run=1,columns='id1',join_type='inner',ans_columns1=['v1'],ans_columns2=['v2'])
  rquestion(x=x,question="small inner on int",join_df=small,run=2,columns='id1',join_type='inner',ans_columns1=['v1'],ans_columns2=['v2'])
  rquestion(x=x,question="medium inner on int",join_df=medium,run=1,columns='id2',join_type='inner',ans_columns1=['v1'],ans_columns2=['v2'])
  rquestion(x=x,question="medium inner on int",join_df=medium,run=2,columns='id2',join_type='inner',ans_columns1=['v1'],ans_columns2=['v2'])
  rquestion(x=x,question="medium outer on int",join_df=medium,run=1,columns='id2',join_type='left',ans_columns1=['v1'],ans_columns2=['v2'])
  rquestion(x=x,question="medium outer on int",join_df=medium,run=2,columns='id2',join_type='left',ans_columns1=['v1'],ans_columns2=['v2'])
  rquestion(x=x,question="medium inner on factor",join_df=medium,run=1,columns='id5',join_type='inner',ans_columns1=['v1'],ans_columns2=['v2'])
  rquestion(x=x,question="medium inner on factor",join_df=medium,run=2,columns='id5',join_type='inner',ans_columns1=['v1'],ans_columns2=['v2'])
  rquestion(x=x,question="big inner on int",join_df=big,run=1,columns='id3',join_type='inner',ans_columns1=['v1'],ans_columns2=['v2'])
  rquestion(x=x,question="big inner on int",join_df=big,run=2,columns='id3',join_type='inner',ans_columns1=['v1'],ans_columns2=['v2'])

  bodo.barrier()
  print(f"joining finished, took  {time.time()-task_init}")

run(src_jn_x, src_jn_small, src_jn_medium, src_jn_big)
exit(0)
