#!/usr/bin/env python

import os
import gc
import sys
import pandas as pd
import bodo
import time
import sys

if(bodo.get_rank()==0):
  print("# groupby-bodo.py")

exec(open("./_helpers/helpers.py").read())

ver = pd.__version__
git = pd.__git_version__
task = "groupby"
solution = "bodo"
fun = ".groupby"
cache = "TRUE"
on_disk = "FALSE"

data_name = os.environ['SRC_DATANAME']
src_grp = os.path.join("data", data_name+".csv")
if(bodo.get_rank()==0):
  print("loading dataset %s" % src_grp)

na_flag = int(data_name.split("_")[3])
if na_flag > 0:
  print("skip due to na_flag>0: #171", file=sys.stderr)
  exit(0) # not yet implemented #171

@bodo.jit
def question9(x,run):
  question = "regression v1 v2 by id2 id4" # q9
  t_start = time.time()
  with bodo.objmode:
    gc.collect()
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
      write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=run, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    gc.collect()

@bodo.jit
def question8(x,run):
  question = "largest two v3 by id6" # q8
  t_start = time.time()
  with bodo.objmode:
    gc.collect()
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
      write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=run, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    gc.collect()

@bodo.jit
def question7(x,run):
  question = "max v1 - min v2 by id3" # q7
  t_start = time.time()
  with bodo.objmode:
    gc.collect()
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
      write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=run, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    gc.collect()

@bodo.jit
def question6(x,question,run,columns,mappers):
  t_start = time.time()
  with bodo.objmode:
    gc.collect()
  ans = x.groupby(columns, as_index=False, sort=False, observed=True, dropna=False).agg(mappers)
  t = time.time() - t_start
  with bodo.objmode(m='float64'):
    m = memory_usage()
  chk = [ans['v3']['median'].sum(), ans['v3']['std'].sum()]
  #chk=[]
  chkt = time.time() - t_start
  in_rows=x.shape[0]
  out_rows=ans.shape[0]
  out_cols=ans.shape[1]
  with bodo.objmode:
    if(bodo.get_rank()==0):
      write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=run, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    #del ans
    gc.collect()

@bodo.jit
def rquestion(x,question,run,columns,mappers,ans_columns):
  t_start = time.time()
  with bodo.objmode:
    gc.collect()
  ans = x.groupby(columns, as_index=False, sort=False, observed=True, dropna=False).agg(mappers)
  t = time.time() - t_start
  with bodo.objmode(m='float64'):
    m = memory_usage()
  chk = [ans[ans_column].sum() for ans_column in ans_columns]
  #chk=[]
  chkt = time.time() - t_start
  in_rows=x.shape[0]
  out_rows=ans.shape[0]
  out_cols=ans.shape[1]
  with bodo.objmode:
    if(bodo.get_rank()==0):
      write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=run, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    gc.collect()

#from datatable import fread # for loading data only, see #47
@bodo.jit(cache=True)
def run(src_grp):
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
  rquestion(x=x,question="sum v1 by id1",run=1,columns='id1',mappers={'v1':'sum'},ans_columns=['v1'])
  rquestion(x=x,question="sum v1 by id1",run=2,columns='id1',mappers={'v1':'sum'},ans_columns=['v1'])
  rquestion(x=x,question="sum v1 by id1:id2",run=1,columns=['id1','id2'],mappers={'v1':'sum'},ans_columns=['v1'])
  rquestion(x=x,question="sum v1 by id1:id2",run=2,columns=['id1','id2'],mappers={'v1':'sum'},ans_columns=['v1'])
  rquestion(x=x,question="sum v1 mean v3 by id3",run=1,columns='id3',mappers={'v1':'sum', 'v3':'mean'},ans_columns=['v1','v3'])
  rquestion(x=x,question="sum v1 mean v3 by id3",run=2,columns='id3',mappers={'v1':'sum', 'v3':'mean'},ans_columns=['v1','v3'])
  rquestion(x=x,question="mean v1:v3 by id4",run=1,columns='id4',mappers={'v1':'mean', 'v2':'mean', 'v3':'mean'},ans_columns=['v1','v2','v3'])
  rquestion(x=x,question="mean v1:v3 by id4",run=2,columns='id4',mappers={'v1':'mean', 'v2':'mean', 'v3':'mean'},ans_columns=['v1','v2','v3'])
  rquestion(x=x,question="sum v1:v3 by id6",run=1,columns='id6',mappers={'v1':'sum', 'v2':'sum', 'v3':'sum'},ans_columns=['v1','v2','v3'])
  rquestion(x=x,question="sum v1:v3 by id6",run=2,columns='id6',mappers={'v1':'sum', 'v2':'sum', 'v3':'sum'},ans_columns=['v1','v2','v3'])

  question6(x=x,question="median v3 sd v3 by id4 id5",run=1,columns=['id4','id5'],mappers={'v3': ['median','std']})
  question6(x=x,question="median v3 sd v3 by id4 id5",run=2,columns=['id4','id5'],mappers={'v3': ['median','std']})

  question7(x,run=1)
  question7(x,run=2)

  question8(x,run=1)
  question8(x,run=2)

  question9(x,run=1)
  question9(x,run=2)

  rquestion(x=x,question="sum v3 count by id1:id6",run=1,columns='id6',mappers={'v3':'sum', 'v1':'size'},ans_columns=['v3','v1'])
  rquestion(x=x,question="sum v3 count by id1:id6",run=2,columns='id6',mappers={'v3':'sum', 'v1':'size'},ans_columns=['v3','v1'])

  bodo.barrier()
  print(f"grouping finished, took  {time.time()-task_init}")

run(src_grp)
exit(0)
