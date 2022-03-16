#!/usr/bin/env python

print("# join-polars.py", flush=True)

import os
import gc
import timeit
import polars as pl

exec(open("./_helpers/helpers.py").read())

ver = pl.__version__
task = "join"
git = ""
solution = "polars"
fun = ".join"
cache = "TRUE"
on_disk = "FALSE"

data_name = os.environ["SRC_DATANAME"]
data_format = os.environ['SRC_FORMAT']
data_location = os.environ['SRC_LOCATION']
endpoint_url = os.environ["AWS_S3_ENDPOINT"]
y_data_name = join_to_tbls(data_name)
if(data_location.lower()=='s3'):
  s3_bucket = os.environ['S3_BUCKET']
  if(data_format.lower()=='parquet'):
    src_jn_x = os.path.join("s3://", s3_bucket, data_name+"_partitioned/*")
    src_jn_big = os.path.join("s3://", s3_bucket, y_data_name[2]+"_partitioned/*")
  else:
    src_jn_x = os.path.join("s3://", s3_bucket, data_name+".csv")
    src_jn_big = os.path.join("s3://", s3_bucket, y_data_name[2]+".csv") 
  src_jn_small = os.path.join("s3://", s3_bucket, y_data_name[0]+".csv")
  src_jn_medium = os.path.join("s3://", s3_bucket, y_data_name[1]+".csv")
else:
  if(data_format.lower()=='parquet'):
    src_jn_x = os.path.join(os.getcwd(), "data", data_name+"_partitioned/*")
    src_jn_big = os.path.join(os.getcwd(), "data", y_data_name[2]+"_partitioned/*")
  else:
    src_jn_x = os.path.join(os.getcwd(), "data", data_name+".csv")
    src_jn_big = os.path.join(os.getcwd(), "data", y_data_name[2]+".csv")
  src_jn_small = os.path.join(os.getcwd(), "data", y_data_name[0]+".csv")
  src_jn_medium = os.path.join(os.getcwd(), "data", y_data_name[1]+".csv")
print("loading datasets " + src_jn_x + ", " + src_jn_small + ", " + src_jn_medium + ", " + src_jn_big, flush=True)

with pl.StringCache():
  print("Starting to read base dataframe")
  task_init = time.time()
  if(data_format.lower()=='parquet'):
    x = pl.read_parquet(src_jn_x, dtype={"id1":pl.Int32, "id2":pl.Int32, "id3":pl.Int32, "v1":pl.Float64}, storage_options={"client_kwargs": {"endpoint_url": endpoint_url}})
  else:
    x = pl.read_csv(src_jn_x, dtype={"id1":pl.Int32, "id2":pl.Int32, "id3":pl.Int32, "v1":pl.Float64}, storage_options={"client_kwargs": {"endpoint_url": endpoint_url}})
  x["id1"] = x["id1"].cast(pl.Int32)
  x["id2"] = x["id2"].cast(pl.Int32)
  x["id4"] = x["id4"].cast(pl.Categorical)
  x["id5"] = x["id5"].cast(pl.Categorical)
  x["id6"] = x["id6"].cast(pl.Categorical)
  print(f"done reading base dataframe in {time.time()-task_init}")
  task_init = time.time()
  print("Starting to read small join dataframe")
  small = pl.read_csv(src_jn_small, dtype={"id1":pl.Int32, "v2":pl.Float64}, storage_options={"client_kwargs": {"endpoint_url": endpoint_url}})
  small["id1"] = small["id1"].cast(pl.Int32)
  small["id4"] = small["id4"].cast(pl.Categorical)
  print(f"done reading small join dataframe in {time.time()-task_init}")
  print("Starting to read medium join dataframe")
  task_init = time.time()
  medium = pl.read_csv(src_jn_medium, dtype={"id1":pl.Int32, "id2":pl.Int32, "v2":pl.Float64}, storage_options={"client_kwargs": {"endpoint_url": endpoint_url}})
  medium["id1"] = medium["id1"].cast(pl.Int32)
  medium["id2"] = medium["id2"].cast(pl.Int32)
  medium["id4"] = medium["id4"].cast(pl.Categorical)
  medium["id5"] = medium["id5"].cast(pl.Categorical)
  print(f"done reading medium join dataframe in {time.time()-task_init}")
  print("Starting to read big join dataframe")
  task_init = time.time()
  #big = pl.read_csv(src_jn_big, dtype={"id1":pl.Int32, "id2":pl.Int32, "id3":pl.Int32, "v2":pl.Float64}, storage_options={"client_kwargs": {"endpoint_url": endpoint_url}})
  big = pl.read_parquet(src_jn_big, dtype={"id1":pl.Int32, "id2":pl.Int32, "id3":pl.Int32, "v2":pl.Float64}, storage_options={"client_kwargs": {"endpoint_url": endpoint_url}})
  big["id1"] = big["id1"].cast(pl.Int32)
  big["id4"] = big["id4"].cast(pl.Categorical)
  big["id5"] = big["id5"].cast(pl.Categorical)
  big["id6"] = big["id6"].cast(pl.Categorical)
  print(f"done reading big join dataframe in {time.time()-task_init}")

task_init = timeit.default_timer()
print("joining...", flush=True)

question = "small inner on int" # q1
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = x.join(small, on="id1")
print(f"Finished 1st run joining in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.join(small, on="id1")
print(f"Finished 2nd run joining in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(f"Finished 2nd run aggregation in {chkt}")
del ans

question = "medium inner on int" # q2
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = x.join(medium, on="id2")
print(f"Finished 1st run joining in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.join(medium, on="id2")
print(f"Finished 2nd run joining in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(f"Finished 2nd run aggregation in {chkt}")
del ans

question = "medium outer on int" # q3
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = x.join(medium, how="left", on="id2")
print(f"Finished 1st run joining in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.join(medium, how="left", on="id2")
print(f"Finished 2nd run joining in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(f"Finished 2nd run aggregation in {chkt}")
del ans

question = "medium inner on factor" # q4
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = x.join(medium, on="id5")
print(f"Finished 1st run joining in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.join(medium, on="id5")
print(f"Finished 2nd run joining in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(f"Finished 2nd run aggregation in {chkt}")
del ans

question = "big inner on int" # q5
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = x.join(big, on="id3")
print(f"Finished 1st run joining in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.join(big, on="id3")
print(f"Finished 2nd run joining in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1"].sum(), ans["v2"].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(f"Finished 2nd run aggregation in {chkt}")
del ans

print("joining finished, took %0.fs" % (timeit.default_timer() - task_init), flush=True)

exit(0)
