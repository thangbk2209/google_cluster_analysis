# tinh resoure usage cua tung timeStamp - cong lai tong resouce usage trong tung file - moi file chua mot diem thoi gian
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
folder_path = '/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/tenSecondsTopJobId/'

dataSchema = StructType([StructField('startTime', StringType(), True),
                         StructField('endTime', StringType(), True),
                         StructField('JobId', LongType(), True),
                         StructField('taskIndex', LongType(), True),
                         StructField('machineId', LongType(), True),
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
                         StructField('sampled_cpu_usage', FloatType(), True)])


for file_name in os.listdir(folder_path):
    df = (
        sql_context.read
        .format('com.databricks.spark.csv')
        .schema(dataSchema)
        .load("%s%s"%(folder_path,file_name))
    )
    
    df.createOrReplaceTempView("dataFrame")
   
    reSourceDf = sql_context.sql("SELECT JobId,count(taskIndex),count(machineId),sum(meanCPUUsage),sum(CMU),sum(AssignMem),sum(unmapped_cache_usage),sum(page_cache_usage), sum(max_mem_usage),sum(mean_diskIO_time),sum(mean_local_disk_space),sum(max_cpu_usage), sum(max_disk_io_time), sum(cpi), sum(mai),sum(sampling_portion),sum(agg_type),sum(sampled_cpu_usage) from dataFrame group by JobId")
    
    reSourceDf.toPandas().to_csv('thangbk2209/resourceTopJopId/%s'%(file_name), index=False, header=None)
sc.stop()