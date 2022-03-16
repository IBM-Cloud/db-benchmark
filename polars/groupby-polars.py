#!/usr/bin/env python

print("# groupby-polars.py", flush=True)

import os
import gc
import timeit
import polars as pl
from polars import col

exec(open("./_helpers/helpers.py").read())

ver = pl.__version__
git = ""
task = "groupby"
solution = "polars"
fun = ".groupby"
cache = "TRUE"
on_disk = "FALSE"

data_name = os.environ["SRC_DATANAME"]
data_format = os.environ['SRC_FORMAT']
data_location = os.environ['SRC_LOCATION']
endpoint_url = os.environ["AWS_S3_ENDPOINT"]
if(data_location.lower()=='s3'):
  s3_bucket = os.environ['S3_BUCKET']
  if(data_format.lower()=='parquet'):
    src_grp = os.path.join("s3://", s3_bucket, data_name+"_partitioned/*")
  else:
    src_grp = os.path.join("s3://", s3_bucket, data_name+".csv")
else:
  if(data_format.lower()=='parquet'):
    src_grp = os.path.join(os.getcwd(), "data", data_name+"_partitioned/*")
  else:
    src_grp = os.path.join(os.getcwd(), "data", data_name+".csv")
print("loading dataset %s" % src_grp, flush=True)

task_init = timeit.default_timer()
with pl.StringCache():
    if(data_format.lower()=='parquet'):
      x = pl.read_parquet(src_grp, dtype={"id4":pl.Int32, "id5":pl.Int32, "id6":pl.Int32, "v1":pl.Int32, "v2":pl.Int32, "v3":pl.Float64}, low_memory=True, storage_options={"client_kwargs": {"endpoint_url": endpoint_url}})
    else:
      x = pl.read_csv(src_grp, dtype={"id4":pl.Int32, "id5":pl.Int32, "id6":pl.Int32, "v1":pl.Int32, "v2":pl.Int32, "v3":pl.Float64}, low_memory=True, storage_options={"client_kwargs": {"endpoint_url": endpoint_url}})
    x["id1"] = x["id1"].cast(pl.Categorical)
    x["id1"].shrink_to_fit(in_place=True)
    x["id2"] = x["id2"].cast(pl.Categorical)
    x["id2"].shrink_to_fit(in_place=True)
    x["id3"] = x["id3"].cast(pl.Categorical)
    x["id3"].shrink_to_fit(in_place=True)

in_rows = x.shape[0]
x = x.lazy()
print(f"done reading base dataframe in {timeit.default_timer() - task_init}")

task_init = timeit.default_timer()
print("grouping...", flush=True)

question = "sum v1 by id1" # q1
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = x.groupby("id1").agg(pl.sum("v1").alias("v1_sum")).collect()
print(f"Finished 1st run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1_sum"].cast(pl.Int64).sum()]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby("id1").agg(pl.sum("v1").alias("v1_sum")).collect()
print(f"Finished 2nd run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1_sum"].cast(pl.Int64).sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(f"Finished 2nd run aggregation in {chkt}")
del ans

question = "sum v1 by id1:id2" # q2
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = x.groupby(["id1","id2"]).agg(pl.sum("v1").alias("v1_sum")).collect()
print(f"Finished 1st run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1_sum"].cast(pl.Int64).sum()]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(["id1","id2"]).agg(pl.sum("v1").alias("v1_sum")).collect()
print(f"Finished 2nd run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["v1_sum"].cast(pl.Int64).sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(f"Finished 2nd run aggregation in {chkt}")
del ans

question = "sum v1 mean v3 by id3" # q3
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = x.groupby("id3").agg([pl.sum("v1").alias("v1_sum"), pl.mean("v3").alias("v3_mean")]).collect()
print(f"Finished 1st run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.lazy().select([pl.col("v1_sum").cast(pl.Int64).sum(), pl.col("v3_mean").sum()]).collect().to_numpy()[0]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby("id3").agg([pl.sum("v1").alias("v1_sum"), pl.mean("v3").alias("v3_mean")]).collect()
print(f"Finished 2nd run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.lazy().select([pl.col("v1_sum").cast(pl.Int64).sum(), pl.col("v3_mean").sum()]).collect().to_numpy()[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(f"Finished 2nd run aggregation in {chkt}")
del ans

question = "mean v1:v3 by id4" # q4
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = x.groupby("id4").agg([pl.mean("v1").alias("v1_mean"), pl.mean("v2").alias("v2_mean"), pl.mean("v3").alias("v3_mean")]).collect()
print(f"Finished 1st run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.lazy().select([pl.col("v1_mean").sum(), pl.col("v2_mean").sum(), pl.col("v3_mean").sum()]).collect().to_numpy()[0]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby("id4").agg([pl.mean("v1").alias("v1_mean"), pl.mean("v2").alias("v2_mean"), pl.mean("v3").alias("v3_mean")]).collect()
print(f"Finished 2nd run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.lazy().select([pl.col("v1_mean").sum(), pl.col("v2_mean").sum(), pl.col("v3_mean").sum()]).collect().to_numpy()[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(f"Finished 2nd run aggregation in {chkt}")
del ans

question = "sum v1:v3 by id6" # q5
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = x.groupby("id6").agg([pl.sum("v1").alias("v1_sum"), pl.sum("v2").alias("v2_sum"), pl.sum("v3").alias("v3_sum")]).collect()
print(f"Finished 1st run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.lazy().select([pl.col("v1_sum").cast(pl.Int64).sum(), pl.col("v2_sum").cast(pl.Int64).sum(), pl.col("v3_sum").sum()]).collect().to_numpy()[0]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby("id6").agg([pl.sum("v1").alias("v1_sum"), pl.sum("v2").alias("v2_sum"), pl.sum("v3").alias("v3_sum")]).collect()
print(f"Finished 2nd run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.lazy().select([pl.col("v1_sum").cast(pl.Int64).sum(), pl.col("v2_sum").cast(pl.Int64).sum(), pl.col("v3_sum").sum()]).collect().to_numpy()[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(f"Finished 2nd run aggregation in {chkt}")
del ans

question = "median v3 sd v3 by id4 id5" # q6
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = x.groupby(["id4","id5"]).agg([pl.median("v3").alias("v3_median"), pl.std("v3").alias("v3_std")]).collect()
print(f"Finished 1st run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.lazy().select([pl.col("v3_median").sum(), pl.col("v3_std").sum()]).collect().to_numpy()[0]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(["id4","id5"]).agg([pl.median("v3").alias("v3_median"), pl.std("v3").alias("v3_std")]).collect()
print(f"Finished 2nd run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.lazy().select([pl.col("v3_median").sum(), pl.col("v3_std").sum()]).collect().to_numpy()[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(f"Finished 2nd run aggregation in {chkt}")
del ans

question = "max v1 - min v2 by id3" # q7
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = x.groupby("id3").agg([(pl.max("v1") - pl.min("v2")).alias("range_v1_v2")]).collect()
print(f"Finished 1st run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["range_v1_v2"].cast(pl.Int64).sum()]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby("id3").agg([(pl.max("v1") - pl.min("v2")).alias("range_v1_v2")]).collect()
print(f"Finished 2nd run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["range_v1_v2"].cast(pl.Int64).sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(f"Finished 2nd run aggregation in {chkt}")
del ans

question = "largest two v3 by id6" # q8
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = x.drop_nulls("v3").sort("v3", reverse=True).groupby("id6").agg(col("v3").head(2).alias("largest2_v3")).explode("largest2_v3").collect()
print(f"Finished 1st run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["largest2_v3"].sum()]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.drop_nulls("v3").sort("v3", reverse=True).groupby("id6").agg(col("v3").head(2).alias("largest2_v3")).explode("largest2_v3").collect()
print(f"Finished 2nd run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["largest2_v3"].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(f"Finished 2nd run aggregation in {chkt}")
del ans

question = "regression v1 v2 by id2 id4" # q9
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = x.groupby(["id2","id4"]).agg((pl.pearson_corr("v1","v2")**2).alias("r2")).collect()
print(f"Finished 1st run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["r2"].sum()]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(["id2","id4"]).agg((pl.pearson_corr("v1","v2")**2).alias("r2")).collect()
print(f"Finished 2nd run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans["r2"].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(f"Finished 2nd run aggregation in {chkt}")
del ans

question = "sum v3 count by id1:id6" # q10
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = x.groupby(["id1","id2","id3","id4","id5","id6"]).agg([pl.sum("v3").alias("v3"), pl.count("v1").alias("count")]).collect()
print(f"Finished 1st run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.lazy().select([pl.col("v3").sum(), pl.col("count").cast(pl.Int64).sum()]).collect().to_numpy()[0]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x.groupby(["id1","id2","id3","id4","id5","id6"]).agg([pl.sum("v3").alias("v3"), pl.count("v1").alias("count")]).collect()
print(f"Finished 2nd run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.lazy().select([pl.col("v3").sum(), pl.col("count").cast(pl.Int64).sum()]).collect().to_numpy()[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(f"Finished 2nd run aggregation in {chkt}")
del ans

print("grouping finished, took %0.fs" % (timeit.default_timer() - task_init), flush=True)

exit(0)
