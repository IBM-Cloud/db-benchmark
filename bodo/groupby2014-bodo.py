#!/usr/bin/env python

import os
import gc
import sys
import pandas as pd
import bodo
import time

exec(open("./_helpers/helpers.py").read())

ver = pd.__version__
git = pd.__git_version__
task = "groupby2014"
solution = "bodo"
fun = ".groupby"
cache = "TRUE"
on_disk = "FALSE"

data_name = os.environ['SRC_DATANAME']
src_grp = os.path.join("data", data_name+".csv")
if(bodo.get_rank()==0):
  print("loading dataset %s" % src_grp, flush=True)

@bodo.jit
def rquestion(x,question,run,columns,mappers,ans_columns):
  with bodo.objmode:
    gc.collect()
  t_start = time.time()
  ans = x.groupby(columns).agg(mappers)
  print(ans.shape)
  t = time.time() - t_start
  with bodo.objmode(m='float64'):
    m = memory_usage()
  t_start = timeit.default_timer()
  chk = [ans[ans_columns].sum()]
  chkt = time.time() - t_start
  with bodo.objmode:
    if(bodo.get_rank()==0):
      write_log(task=task, data=data_name, in_rows=len(x.index), question=question, out_rows=len(ans.index), out_cols=len(ans.index.names)+len(ans.columns), solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    if(run==2 and bodo.get_rank()==0):
      print(ans.head(3))
    print(ans.tail(3))
  del ans

@bodo.jit(cache=True)
def run(src_grp):
  x = pd.read_csv(src_grp)
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

  bodo.barrier()
  print(f"grouping finished, took  {time.time()-task_init}")

run(src_grp)
exit(0)
