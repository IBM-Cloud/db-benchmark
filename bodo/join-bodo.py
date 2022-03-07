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
src_jn_x = os.path.join("data", data_name+".csv")
y_data_name = join_to_tbls(data_name)
src_jn_small = os.path.join("data", y_data_name[0]+".csv")
src_jn_medium = os.path.join("data", y_data_name[1]+".csv")
src_jn_big = os.path.join("data", y_data_name[2]+".csv")

if(bodo.get_rank()==0):
  print("loading datasets " + data_name + ", " + y_data_name[0] + ", " + y_data_name[1] + ", " + y_data_name[2], flush=True)

@bodo.jit
def rquestion(x,question,join_df,run,columns,join_type,ans_column1,ans_column2):
  with bodo.objmode:
    gc.collect()
  t_start = time.time()
  ans = x.merge(join_df, how=join_type, on=columns)
  print(ans.shape)
  t = time.time() - t_start
  with bodo.objmode(m='float64'):
    m = memory_usage()
  t_start = time.time()
  chk = [ans[ans_column1].sum(), ans[ans_column2].sum()]
  chkt = time.time() - t_start
  with bodo.objmode:
    if(bodo.get_rank()==0):
      write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    if(run==2 and bodo.get_rank()==0):
      print(ans.head(3))
      print(ans.tail(3))
  del ans

@bodo.jit(cache=True)
def run(src_jn_x, src_jn_small, src_jn_medium, src_jn_big):
  x = pd.read_csv(src_jn_x)
  print("done reading")
  x['id1'] = x['id1'].astype('Int32')
  x['id2'] = x['id2'].astype('Int32')
  x['id3'] = x['id3'].astype('Int32')
  x['id4'] = x['id4'].astype('category') # remove after datatable#1691
  x['id5'] = x['id5'].astype('category')
  x['id6'] = x['id6'].astype('category')
  x['v1'] = x['v1'].astype('float64')

  small = pd.read_csv(src_jn_small)
  small['id1'] = small['id1'].astype('Int32')
  small['id4'] = small['id4'].astype('category')
  small['v2'] = small['v2'].astype('float64')
  medium = pd.read_csv(src_jn_medium)
  medium['id1'] = medium['id1'].astype('Int32')
  medium['id2'] = medium['id2'].astype('Int32')
  medium['id4'] = medium['id4'].astype('category')
  medium['id5'] = medium['id5'].astype('category')
  medium['v2'] = medium['v2'].astype('float64')
  big = pd.read_csv(src_jn_big)
  big['id1'] = big['id1'].astype('Int32')
  big['id2'] = big['id2'].astype('Int32')
  big['id3'] = big['id3'].astype('Int32')
  big['id4'] = big['id4'].astype('category')
  big['id5'] = big['id5'].astype('category')
  big['id6'] = big['id6'].astype('category')
  big['v2'] = big['v2'].astype('float64')
  print(len(x.index))
  print(len(small.index))
  print(len(medium.index))
  print(len(big.index))
  print("done converting type")
  print(len(x.index))

  task_init = time.time()
  print("joining...")

  rquestion(x=x,question="small inner on int",join_df=small,run=1,columns='id1',join_type='inner',ans_columns1=['v1'], ans_columns2=['v2'])
  rquestion(x=x,question="small inner on int",join_df=small,run=2,columns='id1',join_type='inner',ans_columns1=['v1'], ans_columns2=['v2'])
  rquestion(x=x,question="medium inner on int",join_df=medium,run=1,columns='id2',join_type='inner',ans_columns1=['v1'], ans_columns2=['v2'])
  rquestion(x=x,question="medium inner on int",join_df=medium,run=2,columns='id2',join_type='inner',ans_columns1=['v1'], ans_columns2=['v2'])
  rquestion(x=x,question="medium outer on int",join_df=medium,run=1,columns='id2',join_type='left',ans_columns1=['v1'], ans_columns2=['v2'])
  rquestion(x=x,question="medium outer on int",join_df=medium,run=2,columns='id2',join_type='left',ans_columns1=['v1'], ans_columns2=['v2'])
  rquestion(x=x,question="medium inner on factor",join_df=medium,run=1,columns='id5',join_type='inner',ans_columns1=['v1'], ans_columns2=['v2'])
  rquestion(x=x,question="medium inner on factor",join_df=medium,run=2,columns='id5',join_type='inner',ans_columns1=['v1'], ans_columns2=['v2'])
  rquestion(x=x,question="big inner on int",join_df=big,run=1,columns='id3',join_type='inner',ans_columns1=['v1'], ans_columns2=['v2'])
  rquestion(x=x,question="big inner on int",join_df=big,run=2,columns='id3',join_type='inner',ans_columns1=['v1'], ans_columns2=['v2'])

  bodo.barrier()
  print(f"joining finished, took  {time.time()-task_init}")

run(src_jn_x, src_jn_small, src_jn_medium, src_jn_big)
exit(0)
