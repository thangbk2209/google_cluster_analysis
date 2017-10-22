# Truy xuat cac du lieu ve resource usage cua job id = 6336594489(Job id co so luong task lon nhat) 
# tu cac part trong du lieu google cluster trace
from pyspark.sql.session import SparkSession as spark
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from matplotlib import pyplot
import os
sc = SparkContext(appName="Task_usage")
sql_context = SQLContext(sc)

# folder_path ='/mnt/volume/ggcluster/clusterdata-2011-2/task_usage/'
file_path = '/home/nguyen/spark-lab/spark-2.1.1-bin-hadoop2.7/google_cluster_analysis/data_resource_JobId_6336594489.csv'

dataSchema = StructType([
                         StructField('timeStamp', LongType(), True),
                         StructField('numberOfTaskIndex', LongType(), True),
                         StructField('numberOfMachineId', LongType(), True),
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
                         StructField('sampled_cpu_usage', FloatType(), True)])

df = (
    sql_context.read
    .format('com.databricks.spark.csv')
    .schema(dataSchema)
    .load("%s"%(file_path))
)
df.createOrReplaceTempView("dataFrame")
# df.printSchema()
resourceUsage = sql_context.sql("SELECT meanCPUUsage,CMU,AssignMem  from dataFrame")
# schema_df = ["startTime","numberOfJob"]
resourceUsage.toPandas().to_csv('results/CPU-Ram.csv', index=False, header=None)
# sumCPUUsage.write.save("results/test.csv", format="csv", columns=schema_df)
sc.stop()