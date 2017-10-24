# Tinh tong resource tai cac diem thoi gian chong lan. Cung voi do, sapws xep laij
# resource theo thoi gian tang dan
from pyspark.sql.session import SparkSession as spark
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from matplotlib import pyplot
import os
sc = SparkContext(appName="Task_usage")
sql_context = SQLContext(sc)

# folder_path ='/home/nguyen/spark-lab/spark-2.1.1-bin-hadoop2.7/google_cluster_analysis/results/'
folder_path = '/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/plotTimeSeries/results/'

dataSchema = StructType([
                         StructField('JobId', LongType(), True),
                         StructField('taskIndex', FloatType(), True),
                         StructField('machineId', FloatType(), True),
                         StructField('meanCPUUsage', FloatType(), True),
                         # canonical memory usage
                         StructField('CMU', FloatType(), True),
                         # assigned memory usage
                         StructField('AssignMem', FloatType(), True),
                         # unmapped page cache memory usage
                         StructField('unmapped_cache_usage', FloatType(), True),
                         StructField('page_cache_usage', FloatType(), True),
                         StructField('max_mem_usage', FloatType(), True),
                         StructField('mean_diskIO_time', FloatType(), True),
                         StructField('mean_local_disk_space', FloatType(), True),
                         StructField('max_cpu_usage', FloatType(), True),
                         StructField('max_disk_io_time', FloatType(), True),
                         StructField('cpi', FloatType(), True),
                         StructField('mai', FloatType(), True),
                         StructField('sampling_portion', FloatType(), True),
                         StructField('agg_type', FloatType(), True),
                         StructField('sampled_cpu_usage', FloatType(), True),
                         StructField('time_stamp', FloatType(), True)])

file_name = "all_sample_resource_usage_TopJobId_5minutes.csv"
df = (
    sql_context.read
    .format('com.databricks.spark.csv')
    .schema(dataSchema)
    .load("%s%s"%(folder_path,file_name))
)
df.createOrReplaceTempView("dataFrame")

DataDf = sql_context.sql("SELECT time_stamp,sum(taskIndex),sum(machineId),sum(meanCPUUsage),sum(CMU),sum(AssignMem),sum(unmapped_cache_usage),sum(page_cache_usage), sum(max_mem_usage),sum(mean_diskIO_time),sum(mean_local_disk_space),sum(max_cpu_usage), sum(max_disk_io_time), sum(cpi), sum(mai),sum(sampling_portion),sum(agg_type),sum(sampled_cpu_usage) from dataFrame group by time_stamp order by time_stamp ASC")
print "DataDf.count()= "
print DataDf.count()
DataDf.toPandas().to_csv('/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/plotTimeSeries/results/data_resource_JobId_6336594489_5minutes.csv', index=False, header=None)
sc.stop()