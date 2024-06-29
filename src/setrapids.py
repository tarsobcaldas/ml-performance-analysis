from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os

executorCores = int(os.getenv("EXECUTOR_CORES", "8"))
gpuPerExecutor = 1/executorCores

conf = SparkConf()
conf.set('spark.executor.memory', '100g')
conf.set('spark.driver.memory', '100g')
conf.set('spark.driver.maxResultSize', '10g')
conf.set("spark.executor.resource.gpu.amount", "1")
# conf.set("spark.rapids.sql.concurrentGpuTasks", "4")
conf.set("spark.rapids.memory.pinnedPool.size", "8g")
conf.set("spark.task.resource.gpu.amount", gpuPerExecutor) 
conf.set("spark.rapids.sql.enabled", "true") 

sc = SparkContext(conf=conf).getOrCreate()
