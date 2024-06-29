import numpy as np
import pandas as pd
from benchmark import benchmark, get_results
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as f


home = os.path.expanduser('~')


spark = SparkSession.builder.appName("Rapids test") \
    .config("sparl.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

def read_file_parquet(df=None):
    return spark.read.parquet(f'{home}/ml-performance-analysis/formatted-data/2009', index_col='index')
  
def count(df=None):
    return df.count()
 
def count_index_length(df=None):
    return len(df.columns)
 
def mean(df):
    return df.select(f.mean('fare_amt')).collect()
 
def standard_deviation(df):
    return df.select(f.stddev('fare_amt')).collect()
 
def mean_of_sum(df):
    return df.select(f.mean(df.fare_amt + df.tip_amt)).collect()
 
def sum_columns(df):
    x =  df['fare_amt'] + df['tip_amt']
    return x
    
 
def mean_of_product(df):
    return df.select(f.mean(df.fare_amt * df.tip_amt)).collect()
 
def product_columns(df):
    x =  df.fare_amt * df.tip_amt
    return x
 
def value_counts(df):
    val_counts = df.select('fare_amt').distinct().collect()
    return val_counts
  
def complicated_arithmetic_operation(df):
    df = df.withColumn("start_lat_rad", f.radians(f.col("start_lat")))
    df = df.withColumn("end_lat_rad", f.radians(f.col("end_lat")))
    df = df.withColumn("delta_lon", f.radians(f.col("end_lon") - f.col("start_lon")))
    df = df.withColumn("delta_lat", f.radians(f.col("end_lat") - f.col("start_lat")))
    df = df.withColumn("temp",
       f.sin(df["delta_lat"] / 2) ** 2 +
       f.cos(df["start_lat_rad"]) * f.cos(df["end_lat_rad"]) *
       f.sin(df["delta_lon"] / 2) ** 2)
    df = df.withColumn("result", f.atan2(f.sqrt(df["temp"]), f.sqrt(1-df["temp"]))* 2)
    ret = df.select("index", "result")
    return ret
  
def mean_of_complicated_arithmetic_operation(df):
    df = df.withColumn("start_lat_rad", f.radians(f.col("start_lat")))
    df = df.withColumn("end_lat_rad", f.radians(f.col("end_lat")))
    df = df.withColumn("delta_lon", f.radians(f.col("end_lon") - f.col("start_lon")))
    df = df.withColumn("delta_lat", f.radians(f.col("end_lat") - f.col("start_lat")))
    df = df.withColumn("temp",
       f.sin(df["delta_lat"] / 2) ** 2 +
       f.cos(df["start_lat_rad"]) * f.cos(df["end_lat_rad"]) *
       f.sin(df["delta_lon"] / 2) ** 2)
    df = df.withColumn("result", f.atan2(f.sqrt(df["temp"]), f.sqrt(1-df["temp"]))* 2)
    ret = df.select("result").agg({"result": "avg"}).collect()[0][0]
    return ret
  
def groupby_statistics(df):
    gb = df.groupby('passenger_count').agg(
        f.mean('fare_amt'),
        f.stddev('fare_amt'),
        f.mean('tip_amt'),
        f.stddev('tip_amt')
    )
    return gb
  
def join_count(df, other):
    joined = df.join(other)
    ret = joined.count()
    return  ret
 
def join_data(df, other):
    ret = df.join(other)
    return ret

rapids_data = spark.read.parquet(f'{home}/ml-performance-analysis/formatted-data/2009', index_col = 'index')
# rapids_data = rapids_data.withColumnRenamed('index', 'index_col')
 
rapids_benchmarks = {
    'duration': [],  # in seconds
    'task': [],
}

other = groupby_statistics(rapids_data)
 

benchmark(read_file_parquet, df=None, benchmarks=rapids_benchmarks, name='read file')
benchmark(count, df=rapids_data, benchmarks=rapids_benchmarks, name='count')
benchmark(count_index_length, df=rapids_data, benchmarks=rapids_benchmarks, name='count index length')
benchmark(mean, df=rapids_data, benchmarks=rapids_benchmarks, name='mean')
benchmark(standard_deviation, df=rapids_data, benchmarks=rapids_benchmarks, name='standard deviation')
benchmark(mean_of_sum, df=rapids_data, benchmarks=rapids_benchmarks, name='mean of columns addition')
benchmark(sum_columns, df=rapids_data, benchmarks=rapids_benchmarks, name='addition of columns')
benchmark(mean_of_product, df=rapids_data, benchmarks=rapids_benchmarks, name='mean of columns multiplication')
benchmark(product_columns, df=rapids_data, benchmarks=rapids_benchmarks, name='multiplication of columns')
benchmark(value_counts, df=rapids_data, benchmarks=rapids_benchmarks, name='value counts')
benchmark(complicated_arithmetic_operation, df=rapids_data, benchmarks=rapids_benchmarks, name='complex arithmetic ops')
benchmark(mean_of_complicated_arithmetic_operation, df=rapids_data, benchmarks=rapids_benchmarks, name='mean of complex arithmetic ops')
benchmark(groupby_statistics, df=rapids_data, benchmarks=rapids_benchmarks, name='groupby statistics')
benchmark(join_count, rapids_data, benchmarks=rapids_benchmarks, name='join count', other=other)
benchmark(join_data, rapids_data, benchmarks=rapids_benchmarks, name='join', other=other)

expr_filter = (rapids_data.tip_amt >= 1) & (rapids_data.tip_amt <= 5)
 
def filter_data(df):
    return df[expr_filter]
 
rapids_filtered = filter_data(rapids_data)

benchmark(count, rapids_filtered, benchmarks=rapids_benchmarks, name='filtered count')
benchmark(count_index_length, rapids_filtered, benchmarks=rapids_benchmarks, name='filtered count index length')
benchmark(mean, rapids_filtered, benchmarks=rapids_benchmarks, name='filtered mean')
benchmark(standard_deviation, rapids_filtered, benchmarks=rapids_benchmarks, name='filtered standard deviation')
benchmark(mean_of_sum, rapids_filtered, benchmarks=rapids_benchmarks, name ='filtered mean of columns addition')
benchmark(sum_columns, df=rapids_filtered, benchmarks=rapids_benchmarks, name='filtered addition of columns')
benchmark(mean_of_product, rapids_filtered, benchmarks=rapids_benchmarks, name ='filtered mean of columns multiplication')
benchmark(product_columns, df=rapids_filtered, benchmarks=rapids_benchmarks, name='filtered multiplication of columns')
benchmark(mean_of_complicated_arithmetic_operation, rapids_filtered, benchmarks=rapids_benchmarks, name='filtered mean of complex arithmetic ops')
benchmark(complicated_arithmetic_operation, rapids_filtered, benchmarks=rapids_benchmarks, name='filtered complex arithmetic ops')
benchmark(value_counts, rapids_filtered, benchmarks=rapids_benchmarks, name ='filtered value counts')
benchmark(groupby_statistics, rapids_filtered, benchmarks=rapids_benchmarks, name='filtered groupby statistics')
 
other = groupby_statistics(rapids_data)

benchmark(join_data, rapids_filtered, benchmarks=rapids_benchmarks, name='filtered join', other=other)
benchmark(join_count, rapids_filtered, benchmarks=rapids_benchmarks, name='filtered join count', other=other)
