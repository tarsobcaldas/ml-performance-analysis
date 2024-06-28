import pandas as pd
import numpy as np
import databricks.koalas as ks
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster

print('pandas version: %s' % pd.__version__)
print('numpy version: %s' % np.__version__)
print('koalas version: %s' % ks.__version__)
import dask
print('dask version: %s' % dask.__version__)
 
import time
 
def benchmark(f, df, benchmarks, name, **kwargs):
    """Benchmark the given function against the given DataFrame.
    
    Parameters
    ----------
    f: function to benchmark
    df: data frame
    benchmarks: container for benchmark results
    name: task name
    
    Returns
    -------
    Duration (in seconds) of the given operation
    """
    start_time = time.time()
    ret = f(df, **kwargs)
    benchmarks['duration'].append(time.time() - start_time)
    benchmarks['task'].append(name)
    print(f"{name} took: {benchmarks['duration'][-1]} seconds")
    return benchmarks['duration'][-1]
 
def get_results(benchmarks):
    """Return a pandas DataFrame containing benchmark results."""
    return pd.DataFrame.from_dict(benchmarks)

client = Client()
 
dask_data = dd.read_parquet('/dbfs/FileStore/ks_taxi_parquet', index='index')
 
dask_benchmarks = {
    'duration': [],  # in seconds
    'task': [],
}

def read_file_parquet(df=None):
    return dd.read_parquet('/dbfs/FileStore/ks_taxi_parquet', index='index')
  
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
  
other = groupby_statistics(dask_data)
other.columns = pd.Index([e[0]+'_' + e[1] for e in other.columns.tolist()])
 
def join_count(df, other):
    return len(dd.merge(df, other, left_index=True, right_index=True))
 
def join_data(df, other):
    return dd.merge(df, other, left_index=True, right_index=True).compute()
