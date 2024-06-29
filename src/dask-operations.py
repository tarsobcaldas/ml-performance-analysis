import dask.dataframe as dd
import pandas as pd
import numpy as np
from dask.distributed import Client, LocalCluster
from benchmark import benchmark

import os
home = os.path.expanduser('~')

def read_file_parquet(df=None):
    return dd.read_parquet(f'{home}/ml-performance-analysis/data/2009')
  
def count(df=None):
    return len(df)
 
def count_index_length(df=None):
    return len(df.index)
 
def mean(df):
    return df.fare_amt.mean().compute()
 
def standard_deviation(df):
    return df.fare_amt.std().compute()
 
def mean_of_sum(df):
    return (df.fare_amt + df.tip_amt).mean().compute()
 
def sum_columns(df):
    return (df.fare_amt + df.tip_amt).compute()
 
def mean_of_product(df):
    return (df.fare_amt * df.tip_amt).mean().compute()
 
def product_columns(df):
    return (df.fare_amt * df.tip_amt).compute()
  
def value_counts(df):
    return df.fare_amt.value_counts().compute()
  
def mean_of_complicated_arithmetic_operation(df):
    theta_1 = df.start_lon
    phi_1 = df.start_lat
    theta_2 = df.end_lon
    phi_2 = df.end_lat
    temp = (np.sin((theta_2-theta_1)/2*np.pi/180)**2
           + np.cos(theta_1*np.pi/180)*np.cos(theta_2*np.pi/180) * np.sin((phi_2-phi_1)/2*np.pi/180)**2)
    ret = 2 * np.arctan2(np.sqrt(temp), np.sqrt(1-temp))
    return ret.mean().compute()
  
def complicated_arithmetic_operation(df):
    theta_1 = df.start_lon
    phi_1 = df.start_lat
    theta_2 = df.end_lon
    phi_2 = df.end_lat
    temp = (np.sin((theta_2-theta_1)/2*np.pi/180)**2
           + np.cos(theta_1*np.pi/180)*np.cos(theta_2*np.pi/180) * np.sin((phi_2-phi_1)/2*np.pi/180)**2)
    ret = 2 * np.arctan2(np.sqrt(temp), np.sqrt(1-temp))
    return ret.compute()
  
def groupby_statistics(df):
    return df.groupby(by='passenger_count').agg(
      {
        'fare_amt': ['mean', 'std'], 
        'tip_amt': ['mean', 'std']
      }
    ).compute()
  
 
def join_count(df, other):
    return len(dd.merge(df, other, left_index=True, right_index=True))
 
def join_data(df, other):
    return dd.merge(df, other, left_index=True, right_index=True).compute()

 
dask_data = dd.read_parquet(f'{home}/ml-performance-analysis/data/2009', index='index')
 
dask_benchmarks = {
    'duration': [],  # in seconds
    'task': [],
}


other = groupby_statistics(dask_data)
other.columns = pd.Index([e[0]+'_' + e[1] for e in other.columns.tolist()])

benchmark(read_file_parquet, df=None, benchmarks=dask_benchmarks, name='read file')
benchmark(count, df=dask_data, benchmarks=dask_benchmarks, name='count')
benchmark(count_index_length, df=dask_data, benchmarks=dask_benchmarks, name='count index length')
benchmark(mean, df=dask_data, benchmarks=dask_benchmarks, name='mean')
benchmark(standard_deviation, df=dask_data, benchmarks=dask_benchmarks, name='standard deviation')
benchmark(mean_of_sum, df=dask_data, benchmarks=dask_benchmarks, name='mean of columns addition')
benchmark(sum_columns, df=dask_data, benchmarks=dask_benchmarks, name='addition of columns')
benchmark(mean_of_product, df=dask_data, benchmarks=dask_benchmarks, name='mean of columns multiplication')
benchmark(product_columns, df=dask_data, benchmarks=dask_benchmarks, name='multiplication of columns')
benchmark(value_counts, df=dask_data, benchmarks=dask_benchmarks, name='value counts')
benchmark(mean_of_complicated_arithmetic_operation, df=dask_data, benchmarks=dask_benchmarks, name='mean of complex arithmetic ops')
benchmark(complicated_arithmetic_operation, df=dask_data, benchmarks=dask_benchmarks, name='complex arithmetic ops')
benchmark(groupby_statistics, df=dask_data, benchmarks=dask_benchmarks, name='groupby statistics')
benchmark(join_count, dask_data, benchmarks=dask_benchmarks, name='join count', other=other)
benchmark(join_data, dask_data, benchmarks=dask_benchmarks, name='join', other=other)

expr_filter = (dask_data.tip_amt >= 1) & (dask_data.tip_amt <= 5)
 
def filter_data(df):
    return df[expr_filter]
  
dask_filtered = filter_data(dask_data)

benchmark(count, dask_filtered, benchmarks=dask_benchmarks, name='filtered count')
benchmark(count_index_length, dask_filtered, benchmarks=dask_benchmarks, name='filtered count index length')
benchmark(mean, dask_filtered, benchmarks=dask_benchmarks, name='filtered mean')
benchmark(standard_deviation, dask_filtered, benchmarks=dask_benchmarks, name='filtered standard deviation')
benchmark(mean_of_sum, dask_filtered, benchmarks=dask_benchmarks, name ='filtered mean of columns addition')
benchmark(sum_columns, df=dask_filtered, benchmarks=dask_benchmarks, name='filtered addition of columns')
benchmark(mean_of_product, dask_filtered, benchmarks=dask_benchmarks, name ='filtered mean of columns multiplication')
benchmark(product_columns, df=dask_filtered, benchmarks=dask_benchmarks, name='filtered multiplication of columns')
benchmark(mean_of_complicated_arithmetic_operation, dask_filtered, benchmarks=dask_benchmarks, name='filtered mean of complex arithmetic ops')
benchmark(complicated_arithmetic_operation, dask_filtered, benchmarks=dask_benchmarks, name='filtered complex arithmetic ops')
benchmark(value_counts, dask_filtered, benchmarks=dask_benchmarks, name ='filtered value counts')
benchmark(groupby_statistics, dask_filtered, benchmarks=dask_benchmarks, name='filtered groupby statistics')
 
other = groupby_statistics(dask_filtered)
other.columns = pd.Index([e[0]+'_' + e[1] for e in other.columns.tolist()])
 
benchmark(join_count, dask_filtered, benchmarks=dask_benchmarks, name='filtered join count', other=other)
benchmark(join_data, dask_filtered, benchmarks=dask_benchmarks, name='filtered join', other=other)
