import pandas as pd
import numpy as np
import pyspark as ps
import pyspark.pandas as ks
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
 
print('pandas version: %s' % pd.__version__)
print('numpy version: %s' % np.__version__)
print('pyspark version: %s' % ps.__version__)
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
