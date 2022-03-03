#!/usr/bin/env python


import os
import gc
import sys
import timeit
import pandas as pd
import bodo
import time

if(bodo.get_rank()==0):
  print("# groupby-pandas.py")

exec(open("./_helpers/helpers.py").read())

ver = pd.__version__
git = pd.__git_version__
task = "groupby"
solution = "pandas"
fun = ".groupby"
cache = "TRUE"
on_disk = "FALSE"

data_name = os.environ['SRC_DATANAME']
src_grp = os.path.join("data", data_name+".csv")

if(bodo.get_rank()==0):
  print("loading dataset %s" % src_grp)

na_flag = int(data_name.split("_")[3])
if na_flag > 0:
  #x = pd.read_csv(src_grp, dtype={'id1':'category','id2':'category','id3':'category','id4':'Int32','id5':'Int32','id6':'Int32','v1':'Int32','v2':'Int32','v3':'float64'})
  print("skip due to na_flag>0: #171", file=sys.stderr)
  exit(0) # not yet implemented #171

@bodo.jit
def groupby(df,columns,mapper):
  ans=df.groupby(columns, as_index=False, sort=False, observed=True, dropna=False).agg( mapper)
  return ans
#from datatable import fread # for loading data only, see #47
@bodo.jit(cache=True)
def run():
  print("starting")
  x = pd.read_csv(src_grp)
  print("done reading")
  x['id1'] = x['id1'].astype('category') # remove after datatable#1691
  x['id2'] = x['id2'].astype('category')
  x['id3'] = x['id3'].astype('category')
  x['id4'] = x['id4'].astype('Int32') ## NA-aware types improved after h2oai/datatable#2761 resolved
  x['id5'] = x['id5'].astype('Int32')
  x['id6'] = x['id6'].astype('Int32')
  x['v1'] = x['v1'].astype('Int32')
  x['v2'] = x['v2'].astype('Int32')
  x['v3'] = x['v3'].astype('float64')
  
  print("done converting type")
  print(len(x.index))

  task_init = time.time()
  print("grouping...")

  question = "sum v1 by id1" # q1
  with bodo.objmode:
    gc.collect()
  t_start = time.time()
  ans = x.groupby('id1', as_index=False, sort=False, observed=True, dropna=False).agg({'v1':'sum'})
  print(ans.shape)
  t = time.time() - t_start
  with bodo.objmode(m='float64'):
    m = memory_usage()
  t_start = time.time()
  chk = [ans['v1'].sum()]
  chkt = time.time() - t_start
  in_rows=x.shape[0]
  out_rows=ans.shape[0]
  out_cols=ans.shape[1]
  with bodo.objmode:
    if(bodo.get_rank()==0):
      write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    #del ans
    gc.collect()
  t_start = time.time()
  ans = x.groupby('id1', as_index=False, sort=False, observed=True, dropna=False).agg({'v1':'sum'})
  print(ans.shape)
  t = time.time() - t_start
  with bodo.objmode(m='float64'):
    m = memory_usage()
  t_start = time.time()
  chk = [ans['v1'].sum()]
  chkt = time.time() - t_start
  in_rows=x.shape[0]
  out_rows=ans.shape[0]
  out_cols=ans.shape[1]
  with bodo.objmode:
    if(bodo.get_rank()==0):
      write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
  print(ans.head(3))
  print(ans.tail(3))
  #with bodo.objmode:
    #del ans
  question = "sum v1 by id1:id2" # q2
  with bodo.objmode:
    gc.collect()
  t_start = time.time()
  ans = x.groupby(['id1','id2'], as_index=False, sort=False, observed=True, dropna=False).agg({'v1':'sum'})
  print(ans.shape)
  t = time.time() - t_start
  with bodo.objmode(m='float64'):
    m = memory_usage()
  t_start = time.time()
  chk = [ans['v1'].sum()]
  chkt = time.time() - t_start

  in_rows=x.shape[0]
  out_rows=ans.shape[0]
  out_cols=ans.shape[1]
  with bodo.objmode:
    if(bodo.get_rank()==0):
      write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    #del ans
    gc.collect()
  t_start = time.time()
  ans = x.groupby(['id1','id2'], as_index=False, sort=False, observed=True, dropna=False).agg({'v1':'sum'})
  print(ans.shape)
  t = time.time() - t_start
  with bodo.objmode(m='float64'):
    m = memory_usage()
  t_start = time.time()
  chk = [ans['v1'].sum()]
  chkt = time.time() - t_start
  in_rows=x.shape[0]
  out_rows=ans.shape[0]
  out_cols=ans.shape[1]
  with bodo.objmode:
    if(bodo.get_rank()==0):
      write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
  print(ans.head(3))
  print(ans.tail(3))
  #with bodo.objmode:
    #del ans

  question = "sum v1 mean v3 by id3" # q3
  
  with bodo.objmode:
    gc.collect()
  t_start = time.time()
  ans = x.groupby('id3', as_index=False, sort=False, observed=True, dropna=False).agg({'v1':'sum', 'v3':'mean'})
  print(ans.shape)
  t = time.time() - t_start
  with bodo.objmode(m='float64'):
    m = memory_usage()
  t_start = time.time()
  chk = [ans['v1'].sum(), ans['v3'].sum()]
  chkt = time.time() - t_start
  in_rows=x.shape[0]
  out_rows=ans.shape[0]
  out_cols=ans.shape[1]
  with bodo.objmode:
    if(bodo.get_rank()==0):
      write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    #del ans
    gc.collect()
  t_start = time.time()
  ans = x.groupby('id3', as_index=False, sort=False, observed=True, dropna=False).agg({'v1':'sum', 'v3':'mean'})
  print(ans.shape)
  t = time.time() - t_start
  with bodo.objmode(m='float64'):
    m = memory_usage()
  t_start = time.time()
  chk = [ans['v1'].sum(), ans['v3'].sum()]
  chkt = time.time() - t_start
  in_rows=x.shape[0]
  out_rows=ans.shape[0]
  out_cols=ans.shape[1]
  with bodo.objmode:
    if(bodo.get_rank()==0):
      write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
  print(ans.head(3))
  print(ans.tail(3))
  #with bodo.objmode:
    #del ans

  question = "mean v1:v3 by id4" # q4
  with bodo.objmode:
    gc.collect()
  t_start = time.time()
  ans = x.groupby('id4',  as_index=False, sort=False, observed=True, dropna=False).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})
  print(ans.shape)
  t = time.time() - t_start
  with bodo.objmode(m='float64'):
    m = memory_usage()
  t_start = time.time()
  chk = [ans['v1'].sum(), ans['v2'].sum(), ans['v3'].sum()]
  chkt = time.time() - t_start
  in_rows=x.shape[0]
  out_rows=ans.shape[0]
  out_cols=ans.shape[1]
  with bodo.objmode:
    if(bodo.get_rank()==0):
      write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    #del ans
    gc.collect()
  t_start = time.time()
  ans = x.groupby('id4', as_index=False, sort=False, observed=True, dropna=False).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})
  print(ans.shape)
  t = time.time() - t_start
  with bodo.objmode(m='float64'):
    m = memory_usage()
  t_start = time.time()
  chk = [ans['v1'].sum(), ans['v2'].sum(), ans['v3'].sum()]
  chkt = time.time() - t_start
  in_rows=x.shape[0]
  out_rows=ans.shape[0]
  out_cols=ans.shape[1]
  with bodo.objmode:
    if(bodo.get_rank()==0):
      write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
  print(ans.head(3))
  print(ans.tail(3))
  #with bodo.objmode:
    #del ans

  question = "sum v1:v3 by id6" # q5
  with bodo.objmode:
    gc.collect()
  t_start = time.time()
  ans = x.groupby('id6',  as_index=False, sort=False, observed=True, dropna=False).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})
  print(ans.shape)
  t = time.time() - t_start
  with bodo.objmode(m='float64'):
    m = memory_usage()
  t_start = time.time()
  chk = [ans['v1'].sum(), ans['v2'].sum(), ans['v3'].sum()]
  chkt = time.time() - t_start
  in_rows=x.shape[0]
  out_rows=ans.shape[0]
  out_cols=ans.shape[1]
  with bodo.objmode:
    if(bodo.get_rank()==0):
      write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    #del ans
    gc.collect()
  t_start = time.time()
  ans = x.groupby('id6',  as_index=False, sort=False, observed=True, dropna=False).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})
  print(ans.shape)
  t = time.time() - t_start
  with bodo.objmode(m='float64'):
    m = memory_usage()
  t_start = time.time()
  chk = [ans['v1'].sum(), ans['v2'].sum(), ans['v3'].sum()]
  chkt = time.time() - t_start
  in_rows=x.shape[0]
  out_rows=ans.shape[0]
  out_cols=ans.shape[1]
  with bodo.objmode:
    if(bodo.get_rank()==0):
      write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
  print(ans.head(3))
  print(ans.tail(3))
  #with bodo.objmode:
    #del ans

  question = "median v3 sd v3 by id4 id5" # q6
  with bodo.objmode:
    gc.collect()
  t_start = time.time()
  ans = x.groupby(['id4','id5'], as_index=False, sort=False, observed=True, dropna=False).agg( {'v3': ['median','std']})
  print(ans.shape)
  t = time.time() - t_start
  with bodo.objmode(m='float64'):
    m = memory_usage()
  t_start = time.time()
  chk = [ans['v3']['median'].sum(), ans['v3']['std'].sum()]
  chkt = time.time() - t_start
  in_rows=x.shape[0]
  out_rows=ans.shape[0]
  out_cols=ans.shape[1]
  with bodo.objmode:
    if(bodo.get_rank()==0):
      write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    #del ans
    gc.collect()
  t_start = time.time()
  ans = x.groupby(['id4','id5'],  as_index=False, sort=False, observed=True, dropna=False).agg({'v3': ['median','std']})
  print(ans.shape)
  t = time.time() - t_start
  with bodo.objmode(m='float64'):
    m = memory_usage()
  t_start = time.time()
  chk = [ans['v3']['median'].sum(), ans['v3']['std'].sum()]
  chkt = time.time() - t_start
  in_rows=x.shape[0]
  out_rows=ans.shape[0]
  out_cols=ans.shape[1]
  with bodo.objmode:
    if(bodo.get_rank()==0):
      write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
  print(ans.head(3))
  print(ans.tail(3))
  #with bodo.objmode:
    #del ans
  t_start = time.time()
  ans = x.groupby('id3',  as_index=False, sort=False, observed=True, dropna=False).agg({'v1':'max', 'v2':'min'}).assign(range_v1_v2=lambda x: x['v1']-x['v2'])[['id3','range_v1_v2']]
  print(ans.shape)
  t = time.time() - t_start
  with bodo.objmode(m='float64'):
    m = memory_usage()
  t_start = time.time()
  chk = [ans['range_v1_v2'].sum()]
  chkt = time.time() - t_start
  in_rows=x.shape[0]
  out_rows=ans.shape[0]
  out_cols=ans.shape[1]
  with bodo.objmode:
    if(bodo.get_rank()==0):
      write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    gc.collect()
  t_start = time.time()
  ans = x.groupby('id3',  as_index=False, sort=False, observed=True, dropna=False).agg({'v1':'max', 'v2':'min'}).assign(range_v1_v2=lambda x: x['v1']-x['v2'])[['id3','range_v1_v2']]
  print(ans.shape)
  t = time.time() - t_start
  with bodo.objmode(m='float64'):
    m = memory_usage()
  t_start = time.time()
  chk = [ans['range_v1_v2'].sum()]
  chkt = time.time() - t_start
  in_rows=x.shape[0]
  out_rows=ans.shape[0]
  out_cols=ans.shape[1]
  with bodo.objmode:
    if(bodo.get_rank()==0):
      write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
  print(ans.head(3))
  print(ans.tail(3))
  question = "max v1 - min v2 by id3" # q7
  with bodo.objmode:
    gc.collect()
  
  question = "largest two v3 by id6" # q8
  with bodo.objmode:
    gc.collect()
  t_start = time.time()
  ans = x[~x['v3'].isna()][['id6','v3']].sort_values('v3', ascending=False).groupby('id6', as_index=False, sort=False, observed=True, dropna=False).head(2)
  ans.reset_index(drop=True, inplace=True)
  print(ans.shape)
  t = time.time() - t_start
  with bodo.objmode(m='float64'):
    m = memory_usage()
  t_start = time.time()
  chk = [ans['v3'].sum()]
  chkt = time.time() - t_start
  in_rows=x.shape[0]
  out_rows=ans.shape[0]
  out_cols=ans.shape[1]
  with bodo.objmode:
    if(bodo.get_rank()==0):
      write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
  #del ans
    gc.collect()
  t_start = time.time()
  ans = x[~x['v3'].isna()][['id6','v3']].sort_values('v3', ascending=False).groupby('id6', as_index=False, sort=False, observed=True, dropna=False).head(2)
  ans.reset_index(drop=True, inplace=True)
  print(ans.shape)
  t = time.time() - t_start
  with bodo.objmode(m='float64'):
    m = memory_usage()
  t_start = time.time()
  chk = [ans['v3'].sum()]
  chkt = time.time() - t_start
  in_rows=x.shape[0]
  out_rows=ans.shape[0]
  out_cols=ans.shape[1]
  with bodo.objmode:
    if(bodo.get_rank()==0):
      write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
  print(ans.head(3))
  print(ans.tail(3))

  question = "regression v1 v2 by id2 id4" # q9
  #corr().iloc[0::2][['v2']]**2 # on 1e8,k=1e2 slower, 76s vs 47s
  with bodo.objmode:
    gc.collect()
  t_start = time.time()
  ans = x[['id2','id4','v1','v2']].groupby(['id2','id4'], as_index=False, sort=False, observed=True, dropna=False).apply(lambda x: pd.Series({'r2': x.corr()['v1']['v2']**2}))
  print(ans.shape)
  t = time.time() - t_start
  with bodo.objmode(m='float64'):
    m = memory_usage()
  t_start = time.time()
  chk = [ans['r2'].sum()]
  chkt = time.time() - t_start
  in_rows=x.shape[0]
  out_rows=ans.shape[0]
  out_cols=ans.shape[1]
  with bodo.objmode:
    if(bodo.get_rank()==0):
      write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    #del ans
    gc.collect()
  t_start = time.time()
  ans = x[['id2','id4','v1','v2']].groupby(['id2','id4'], as_index=False, sort=False, observed=True, dropna=False).apply(lambda x: pd.Series({'r2': x.corr()['v1']['v2']**2}))
  print(ans.shape)
  t = time.time() - t_start
  with bodo.objmode(m='float64'):
    m = memory_usage()
  t_start = time.time()
  chk = [ans['r2'].sum()]
  chkt = time.time() - t_start
  in_rows=x.shape[0]
  out_rows=ans.shape[0]
  out_cols=ans.shape[1]
  with bodo.objmode:
    if(bodo.get_rank()==0):
      write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
  print(ans.head(3))
  print(ans.tail(3))
  #del ans

  question = "sum v3 count by id1:id6" # q10
  with bodo.objmode:
    gc.collect()
  t_start = time.time()
  ans = x.groupby(['id1','id2','id3','id4','id5','id6'], as_index=False, sort=False, observed=True, dropna=False).agg( {'v3':'sum', 'v1':'size'})
  print(ans.shape)
  t = time.time() - t_start
  with bodo.objmode(m='float64'):
    m = memory_usage()
  t_start = time.time()
  chk = [ans['v3'].sum(), ans['v1'].sum()]
  chkt = time.time() - t_start
  in_rows=x.shape[0]
  out_rows=ans.shape[0]
  out_cols=ans.shape[1]
  with bodo.objmode:
    if(bodo.get_rank()==0):
      write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=out_cols, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    gc.collect()
  t_start = time.time()
  ans = x.groupby(['id1','id2','id3','id4','id5','id6'],  as_index=False, sort=False, observed=True, dropna=False).agg({'v3':'sum', 'v1':'size'})
  print(ans.shape)
  t = time.time() - t_start
  with bodo.objmode(m='float64'):
    m = memory_usage()
  t_start = time.time()
  chk = [ans['v3'].sum(), ans['v1'].sum()]
  chkt = time.time() - t_start
  in_rows=x.shape[0]
  out_rows=ans.shape[0]
  out_cols=ans.shape[1]
  with bodo.objmode:
    if(bodo.get_rank()==0):
      write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=out_rows, out_cols=out_cols, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
  print(ans.head(3))
  print(ans.tail(3))
  bodo.barrier()
  print(f"grouping finished, took  {time.time()-task_init}")
run()
exit(0)
