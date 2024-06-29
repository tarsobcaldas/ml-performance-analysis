import os
import databricks.koalas as ks
import numpy as np
import pandas as pd
from benchmark import benchmark


home = os.path.expanduser('~')

def read_file_parquet(df=None):
    return ks.read_parquet(f'{home}/ml-performance-analysis/data/2009', index_col='index')
  
def count(df=None):
    return len(df)
 
def count_index_length(df=None):
    return len(df.index)
 
def mean(df):
    return df.fare_amt.mean()
 
def standard_deviation(df):
    return df.fare_amt.std()
 
def mean_of_sum(df):
    return (df.fare_amt + df.tip_amt).mean()
 
def sum_columns(df):
    x = df.fare_amt + df.tip_amt
    x.to_pandas()
    return x
 
def mean_of_product(df):
    return (df.fare_amt * df.tip_amt).mean()
 
def product_columns(df):
    x = df.fare_amt * df.tip_amt
    x.to_pandas()
    return x
 
def value_counts(df):
    val_counts = df.fare_amt.value_counts()
    val_counts.to_pandas()
    return val_counts
  
def complicated_arithmetic_operation(df):
    theta_1 = df.start_lon
    phi_1 = df.start_lat
    theta_2 = df.end_lon
    phi_2 = df.end_lat
    temp = (np.sin((theta_2 - theta_1) / 2 * np.pi / 180) ** 2
           + np.cos(theta_1 * np.pi / 180) * np.cos(theta_2 * np.pi / 180) * np.sin((phi_2 - phi_1) / 2 * np.pi / 180) ** 2)
    ret = np.multiply(np.arctan2(np.sqrt(temp), np.sqrt(1-temp)),2)
    ret.to_pandas()
    return ret
  
def mean_of_complicated_arithmetic_operation(df):
    theta_1 = df.start_lon
    phi_1 = df.start_lat
    theta_2 = df.end_lon
    phi_2 = df.end_lat
    temp = (np.sin((theta_2 - theta_1) / 2 * np.pi / 180) ** 2
           + np.cos(theta_1 * np.pi / 180) * np.cos(theta_2 * np.pi / 180) * np.sin((phi_2 - phi_1) / 2 * np.pi / 180) ** 2)
    ret = np.multiply(np.arctan2(np.sqrt(temp), np.sqrt(1-temp)),2) 
    return ret.mean()
  
def groupby_statistics(df):
    gb = df.groupby(by='passenger_count').agg(
      {
        'fare_amt': ['mean', 'std'], 
        'tip_amt': ['mean', 'std']
      }
    )
    gb.to_pandas()
    return gb
  
def join_count(df, other):
    return len(df.merge(other.spark.hint("broadcast"), left_index=True, right_index=True))
 
def join_data(df, other):
    ret = df.merge(other.spark.hint("broadcast"), left_index=True, right_index=True)
    ret.to_pandas()
    return ret

np.int = int
np.bool = bool
np.float = float

koalas_data = ks.read_parquet(f'{home}/ml-performance-analysis/data/2009', index_col='index')
 
koalas_benchmarks = {
    'duration': [],  # in seconds
    'task': [],
}

other = ks.DataFrame(groupby_statistics(koalas_data).to_pandas())
other.columns = pd.Index([e[0]+'_' + e[1] for e in other.columns.tolist()])
 

benchmark(read_file_parquet, df=None, benchmarks=koalas_benchmarks, name='read file')
benchmark(count, df=koalas_data, benchmarks=koalas_benchmarks, name='count')
benchmark(count_index_length, df=koalas_data, benchmarks=koalas_benchmarks, name='count index length')
benchmark(mean, df=koalas_data, benchmarks=koalas_benchmarks, name='mean')
benchmark(standard_deviation, df=koalas_data, benchmarks=koalas_benchmarks, name='standard deviation')
benchmark(mean_of_sum, df=koalas_data, benchmarks=koalas_benchmarks, name='mean of columns addition')
benchmark(sum_columns, df=koalas_data, benchmarks=koalas_benchmarks, name='addition of columns')
benchmark(mean_of_product, df=koalas_data, benchmarks=koalas_benchmarks, name='mean of columns multiplication')
benchmark(product_columns, df=koalas_data, benchmarks=koalas_benchmarks, name='multiplication of columns')
benchmark(value_counts, df=koalas_data, benchmarks=koalas_benchmarks, name='value counts')
benchmark(complicated_arithmetic_operation, df=koalas_data, benchmarks=koalas_benchmarks, name='complex arithmetic ops')
benchmark(mean_of_complicated_arithmetic_operation, df=koalas_data, benchmarks=koalas_benchmarks, name='mean of complex arithmetic ops')
benchmark(groupby_statistics, df=koalas_data, benchmarks=koalas_benchmarks, name='groupby statistics')
benchmark(join_count, koalas_data, benchmarks=koalas_benchmarks, name='join count', other=other)
benchmark(join_data, koalas_data, benchmarks=koalas_benchmarks, name='join', other=other)

expr_filter = (koalas_data.tip_amt >= 1) & (koalas_data.tip_amt <= 5)
 
def filter_data(df):
    return df[expr_filter]
 
koalas_filtered = filter_data(koalas_data)

benchmark(count, koalas_filtered, benchmarks=koalas_benchmarks, name='filtered count')
benchmark(count_index_length, koalas_filtered, benchmarks=koalas_benchmarks, name='filtered count index length')
benchmark(mean, koalas_filtered, benchmarks=koalas_benchmarks, name='filtered mean')
benchmark(standard_deviation, koalas_filtered, benchmarks=koalas_benchmarks, name='filtered standard deviation')
benchmark(mean_of_sum, koalas_filtered, benchmarks=koalas_benchmarks, name ='filtered mean of columns addition')
benchmark(sum_columns, df=koalas_filtered, benchmarks=koalas_benchmarks, name='filtered addition of columns')
benchmark(mean_of_product, koalas_filtered, benchmarks=koalas_benchmarks, name ='filtered mean of columns multiplication')
benchmark(product_columns, df=koalas_filtered, benchmarks=koalas_benchmarks, name='filtered multiplication of columns')
benchmark(mean_of_complicated_arithmetic_operation, koalas_filtered, benchmarks=koalas_benchmarks, name='filtered mean of complex arithmetic ops')
benchmark(complicated_arithmetic_operation, koalas_filtered, benchmarks=koalas_benchmarks, name='filtered complex arithmetic ops')
benchmark(value_counts, koalas_filtered, benchmarks=koalas_benchmarks, name ='filtered value counts')
benchmark(groupby_statistics, koalas_filtered, benchmarks=koalas_benchmarks, name='filtered groupby statistics')
 
other = ks.DataFrame(groupby_statistics(koalas_filtered).to_pandas())
other.columns = pd.Index([e[0]+'_' + e[1] for e in other.columns.tolist()])
benchmark(join_data, koalas_filtered, benchmarks=koalas_benchmarks, name='filtered join', other=other)
benchmark(join_count, koalas_filtered, benchmarks=koalas_benchmarks, name='filtered join count', other=other)
