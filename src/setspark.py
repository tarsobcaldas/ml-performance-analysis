from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf()
conf.set('spark.executor.memory', '100g')
conf.set('spark.driver.memory', '100g')
conf.set('spark.driver.maxResultSize', '10g')
spark_context = SparkContext(conf=conf).getOrCreate()

builder = SparkSession.builder.appName("Koalas")
builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "true")
builder.getOrCreate()
