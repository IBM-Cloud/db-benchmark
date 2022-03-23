#!/usr/bin/env python

print("# groupby-datafusion.py", flush=True)

import os
import gc
import timeit
import datafusion as df
from datafusion import functions as f
from datafusion import col
from pyarrow import csv as pacsv
from pyarrow import parquet as paparquet

exec(open("./_helpers/helpers.py").read())

def ans_shape(batches):
    rows, cols = 0, 0
    for batch in batches:
        rows += batch.num_rows
        if cols == 0:
            cols = batch.num_columns
        else:
            assert(cols == batch.num_columns)
    
    return rows, cols

# ver = df.__version__
ver = "6.0.0"
git = ""
task = "groupby"
solution = "datafusion"
fun = ".groupby"
cache = "TRUE"
on_disk = "FALSE"


data_name = os.environ['SRC_DATANAME']
data_format = os.environ['SRC_FORMAT']
if(data_format.lower()=='parquet'):
  src_grp = os.path.join(os.getcwd(), "data", data_name+"_partitioned/")
else:
  src_grp = os.path.join(os.getcwd(), "data", data_name+".csv")
print("loading dataset %s" % src_grp, flush=True)

task_init = timeit.default_timer()
if(data_format.lower()=='parquet'):
  data = paparquet.read_table(src_grp)
else:
  data = pacsv.read_csv(src_grp, convert_options=pacsv.ConvertOptions(auto_dict_encode=True))
print(f"done reading base dataframe in {timeit.default_timer() - task_init}")

ctx = df.ExecutionContext()
ctx.register_record_batches("x", [data.to_batches()])

in_rows = data.num_rows
print(in_rows, flush=True)

task_init = timeit.default_timer()
print("grouping...", flush=True)

question = "sum v1 by id1" # q1
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id1, SUM(v1) AS v1 FROM x GROUP BY id1").collect()
shape = ans_shape(ans)
print(f"Finished 1st run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v1"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id1, SUM(v1) AS v1 FROM x GROUP BY id1").collect()
shape = ans_shape(ans)
print(f"Finished 2nd run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v1"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
print(f"Finished 2nd run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans

question = "sum v1 by id1:id2" # q2
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id1, id2, SUM(v1) AS v1 FROM x GROUP BY id1, id2").collect()
shape = ans_shape(ans)
print(f"Finished 1st run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v1"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id1, id2, SUM(v1) AS v1 FROM x GROUP BY id1, id2").collect()
shape = ans_shape(ans)
print(f"Finished 2nd run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v1"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
print(f"Finished 2nd run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans

question = "sum v1 mean v3 by id3" # q3
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id3, SUM(v1) AS v1, AVG(v3) AS v3 FROM x GROUP BY id3").collect()
shape = ans_shape(ans)
print(f"Finished 1st run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v1")), f.sum(col("v3"))]).collect()[0].to_pandas().to_numpy()[0]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id3, SUM(v1) AS v1, AVG(v3) AS v3 FROM x GROUP BY id3").collect()
shape = ans_shape(ans)
print(f"Finished 2nd run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v1")), f.sum(col("v3"))]).collect()[0].to_pandas().to_numpy()[0]
chkt = timeit.default_timer() - t_start
print(f"Finished 2nd run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans

question = "mean v1:v3 by id4" # q4
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id4, AVG(v1) AS v1, AVG(v2) AS v2, AVG(v3) AS v3 FROM x GROUP BY id4").collect()
shape = ans_shape(ans)
print(f"Finished 1st run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v1")), f.sum(col("v2")), f.sum(col("v3"))]).collect()[0].to_pandas().to_numpy()[0]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id4, AVG(v1) AS v1, AVG(v2) AS v2, AVG(v3) AS v3 FROM x GROUP BY id4").collect()
shape = ans_shape(ans)
print(f"Finished 2nd run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v1")), f.sum(col("v2")), f.sum(col("v3"))]).collect()[0].to_pandas().to_numpy()[0]
chkt = timeit.default_timer() - t_start
print(f"Finished 2nd run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans

question = "sum v1:v3 by id6" # q5
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id6, SUM(v1) AS v1, SUM(v2) AS v2, SUM(v3) AS v3 FROM x GROUP BY id6").collect()
shape = ans_shape(ans)
print(f"Finished 1st run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v1")), f.sum(col("v2")), f.sum(col("v3"))]).collect()[0].to_pandas().to_numpy()[0]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id6, SUM(v1) AS v1, SUM(v2) AS v2, SUM(v3) AS v3 FROM x GROUP BY id6").collect()
shape = ans_shape(ans)
print(f"Finished 2nd run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v1")), f.sum(col("v2")), f.sum(col("v3"))]).collect()[0].to_pandas().to_numpy()[0]
chkt = timeit.default_timer() - t_start
print(f"Finished 2nd run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans

question = "max v1 - min v2 by id3" # q7
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id3, MAX(v1) - MIN(v2) AS range_v1_v2 FROM x GROUP BY id3").collect()
shape = ans_shape(ans)
print(f"Finished 1st run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("range_v1_v2"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id3, MAX(v1) - MIN(v2) AS range_v1_v2 FROM x GROUP BY id3").collect()
shape = ans_shape(ans)
print(f"Finished 2nd run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("range_v1_v2"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
print(f"Finished 2nd run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans

question = "largest two v3 by id6" # q8
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id6, v3 from (SELECT id6, v3, row_number() OVER (PARTITION BY id6 ORDER BY v3 DESC) AS row FROM x) t WHERE row <= 2").collect()
shape = ans_shape(ans)
print(f"Finished 1st run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v3"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id6, v3 from (SELECT id6, v3, row_number() OVER (PARTITION BY id6 ORDER BY v3 DESC) AS row FROM x) t WHERE row <= 2").collect()
shape = ans_shape(ans)
print(f"Finished 2nd run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v3"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
print(f"Finished 2nd run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans

question = "sum v3 count by id1:id6" # q10
gc.collect()
print("\nRunning: " + question, flush=True)
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id1, id2, id3, id4, id5, id6, SUM(v3) as v3, COUNT(*) AS cnt FROM x GROUP BY id1, id2, id3, id4, id5, id6").collect()
shape = ans_shape(ans)
print(f"Finished 1st run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v3")), f.sum(col("cnt"))]).collect()[0].to_pandas().to_numpy()[0]
chkt = timeit.default_timer() - t_start
print(f"Finished 1st run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id1, id2, id3, id4, id5, id6, SUM(v3) as v3, COUNT(*) AS cnt FROM x GROUP BY id1, id2, id3, id4, id5, id6").collect()
shape = ans_shape(ans)
print(f"Finished 2nd run grouping in {timeit.default_timer() - t_start}")
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v3")), f.sum(col("cnt"))]).collect()[0].to_pandas().to_numpy()[0]
chkt = timeit.default_timer() - t_start
print(f"Finished 2nd run aggregation in {chkt}")
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)

print("grouping finished, took %0.fs" % (timeit.default_timer() - task_init), flush=True)

exit(0)
