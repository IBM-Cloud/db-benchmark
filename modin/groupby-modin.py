#!/usr/bin/env python

print("# groupby-modin.py", flush=True)

import os
import gc
import timeit
import modin as modin
import modin.pandas as pd
import modin.config as cfg
import ray
cfg.Engine.put('ray')
cfg.StorageFormat.put('pandas')

exec(open("./_helpers/helpers.py").read())

ver = modin.__version__
git = ""
task = "groupby"
solution = "modin"
fun = ".groupby"
cache = "TRUE"
on_disk = "FALSE"

ray.init()

data_name = os.environ['SRC_DATANAME']
data_format = os.environ['SRC_FORMAT']
data_location = os.environ['SRC_LOCATION']
endpoint_url = os.environ["AWS_S3_ENDPOINT"]
if(data_location.lower()=='s3'):
  s3_bucket = os.environ['S3_BUCKET']
  if(data_format.lower()=='parquet'):
    src_grp = os.path.join("s3://", s3_bucket, data_name+"_partitioned/")
  else:
    src_grp = os.path.join("s3://", s3_bucket, data_name+".csv")
else:
  if(data_format.lower()=='parquet'):
    src_grp = os.path.join(os.getcwd(), "data", data_name+"_partitioned/")
  else:
    src_grp = os.path.join(os.getcwd(), "data", data_name+".csv")
print("loading dataset %s" % src_grp, flush=True)

task_init = timeit.default_timer()
if(data_format.lower()=='parquet'):
  if(data_location.lower()=='s3'):
    x = pd.read_parquet(src_grp, storage_options={"client_kwargs": {"endpoint_url": endpoint_url}})
  else:
    x = pd.read_parquet(src_grp)
else:
  if(data_location.lower()=='s3'):
    x = pd.read_csv(src_grp, dtype={'id1':'category', 'id2':'category', 'id3':'category'}, storage_options={"client_kwargs": {"endpoint_url": endpoint_url}})
  else:
    x = pd.read_csv(src_grp, dtype={'id1':'category', 'id2':'category', 'id3':'category'})
print(f"done reading base dataframe in {timeit.default_timer() - task_init}")

task_init = timeit.default_timer()
print("grouping...", flush=True)

question = "sum v1 by id1" # q1
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = x.groupby(['id1'], observed=True).agg({'v1':'sum'})
ans.reset_index(inplace=True) # #68
print(f"Finished 1st run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum()]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1'], observed=True).agg({'v1':'sum'})
ans.reset_index(inplace=True)
print(f"Finished 2nd run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum()]
chkt = timeit.default_timer() - t_start
print(f"Finished 2nd run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans

question = "sum v1 by id1:id2" # q2
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = x.groupby(['id1','id2'], observed=True).agg({'v1':'sum'})
ans.reset_index(inplace=True)
print(f"Finished 1st run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum()]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1','id2'], observed=True).agg({'v1':'sum'})
ans.reset_index(inplace=True)
print(f"Finished 2nd run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(f"Finished 2nd run aggregation in {chkt}")
del ans

question = "sum v1 mean v3 by id3" # q3
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
t_start = timeit.default_timer()
ans = x.groupby(['id3'], observed=True).agg({'v1':'sum', 'v3':'mean'})
ans.reset_index(inplace=True)
print(f"Finished 1st run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum(), ans['v3'].sum()]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id3'], observed=True).agg({'v1':'sum', 'v3':'mean'})
ans.reset_index(inplace=True)
print(f"Finished 2nd run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum(), ans['v3'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(f"Finished 2nd run aggregation in {chkt}")
del ans

question = "mean v1:v3 by id4" # q4
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = x.groupby(['id4'], observed=True).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})
ans.reset_index(inplace=True)
print(f"Finished 1st run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum(), ans['v2'].sum(), ans['v3'].sum()]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id4'], observed=True).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'})
ans.reset_index(inplace=True)
print(f"Finished 2nd run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum(), ans['v2'].sum(), ans['v3'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(f"Finished 2nd run aggregation in {chkt}")
del ans

question = "sum v1:v3 by id6" # q5
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = x.groupby(['id6'], observed=True).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})
ans.reset_index(inplace=True)
print(f"Finished 1st run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum(), ans['v2'].sum(), ans['v3'].sum()]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id6'], observed=True).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'})
ans.reset_index(inplace=True)
print(f"Finished 2nd run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1'].sum(), ans['v2'].sum(), ans['v3'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(f"Finished 2nd run aggregation in {chkt}")
del ans

question = "median v3 sd v3 by id4 id5" # q6
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = x.groupby(['id4','id5'], observed=True).agg({'v3': ['median','std']})
ans.reset_index(inplace=True)
print(f"Finished 1st run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v3']['median'].sum(), ans['v3']['std'].sum()]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id4','id5'], observed=True).agg({'v3': ['median','std']})
ans.reset_index(inplace=True)
print(f"Finished 2nd run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v3']['median'].sum(), ans['v3']['std'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(f"Finished 2nd run aggregation in {chkt}")
del ans

question = "max v1 - min v2 by id3" # q7
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = x.groupby(['id3'], observed=True).agg({'v1': 'max', 'v2': 'min'}).assign(range_v1_v2=lambda x: x['v1'] - x['v2'])[['range_v1_v2']]
ans.reset_index(inplace=True)
print(f"Finished 1st run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['range_v1_v2'].sum()]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id3'], observed=True).agg({'v1': 'max', 'v2': 'min'}).assign(range_v1_v2=lambda x: x['v1'] - x['v2'])[['range_v1_v2']]
ans.reset_index(inplace=True)
print(f"Finished 2nd run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['range_v1_v2'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(f"Finished 2nd run aggregation in {chkt}")
del ans

question = "largest two v3 by id6" # q8
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = x[['id6','v3']].sort_values('v3', ascending=False).groupby(['id6'], observed=True).head(2)
ans.reset_index(drop=True, inplace=True)
print(f"Finished 1st run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v3'].sum()]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x[['id6','v3']].sort_values('v3', ascending=False).groupby(['id6'], observed=True).head(2)
ans.reset_index(drop=True, inplace=True)
print(f"Finished 2nd run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v3'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(f"Finished 2nd run aggregation in {chkt}")
del ans

# Modin fails in this groupby with parquet based data frames
if(data_format.lower()!='parquet'):
    question = "regression v1 v2 by id2 id4" # q9
    #ans = x[['id2','id4','v1','v2']].groupby(['id2','id4']).corr().iloc[0::2][['v2']]**2 # slower, 76s vs 47s on 1e8 1e2
    gc.collect()
    print("\nRunning: " + question, flush=True)
    t_start = timeit.default_timer()
    ans = x[['id2','id4','v1','v2']].groupby(['id2','id4'], observed=True).apply(lambda x: pd.Series({'r2': x.corr()['v1']['v2']**2}))
    ans.reset_index(inplace=True)
    print(f"Finished 1st run grouping in {timeit.default_timer() - t_start}")
    t = timeit.default_timer() - t_start
    m = memory_usage()
    t_start = timeit.default_timer()
    chk = [ans['r2'].sum()]
    chkt = timeit.default_timer() - t_start
    print(f"Finished 1st run aggregation in {chkt}")
    write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    del ans
    gc.collect()
    t_start = timeit.default_timer()
    ans = x[['id2','id4','v1','v2']].groupby(['id2','id4'], observed=True).apply(lambda x: pd.Series({'r2': x.corr()['v1']['v2']**2}))
    ans.reset_index(inplace=True)
    print(f"Finished 2nd run grouping in {timeit.default_timer() - t_start}")
    t = timeit.default_timer() - t_start
    m = memory_usage()
    t_start = timeit.default_timer()
    chk = [ans['r2'].sum()]
    chkt = timeit.default_timer() - t_start
    write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
    print(f"Finished 2nd run aggregation in {chkt}")
    del ans

question = "sum v3 count by id1:id6" # q10
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = x.groupby(['id1','id2','id3','id4','id5','id6'], observed=True).agg({'v3':'sum', 'v1':'count'})
ans.reset_index(inplace=True)
print(f"Finished 1st run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v3'].sum(), ans['v1'].sum()]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(['id1','id2','id3','id4','id5','id6'], observed=True).agg({'v3':'sum', 'v1':'count'})
ans.reset_index(inplace=True)
print(f"Finished 2nd run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v3'].sum(), ans['v1'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(f"Finished 2nd run aggregation in {chkt}")
del ans

print("grouping finished, took %0.fs" % (timeit.default_timer()-task_init), flush=True)

exit(0)
