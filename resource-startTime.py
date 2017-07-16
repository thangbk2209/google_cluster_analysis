
from pyspark.sql.session import SparkSession as spark
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from matplotlib import pyplot
import os
sc = SparkContext(appName="Task_usage")
sql_context = SQLContext(sc)
startTimeDf = pd.read_csv('/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/TopJobId/TopJobId-startTime.csv', header=None)
# split dataset
startTimeArr = startTimeDf.values

# folder_path ='/mnt/volume/ggcluster/clusterdata-2011-2/task_usage/'
folder_path = '/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/TopJobId/TopJobId.csv'

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

# for file_name in os.listdir(folder_path):
df = (
    sql_context.read
    .format('com.databricks.spark.csv')
    .schema(dataSchema)
    .load("%s"%(folder_path))
)
df.createOrReplaceTempView("dataFrame")
# df.printSchema()
for i in range(len(startTimeArr))
    sumCPUUsage = sql_context.sql("SELECT startTime/1000000,endTime/1000000,JobId,meanCPUUsage,CMU,AssignMem,unmap_page_cache_memory_ussage,page_cache_usage,mean_diskIO_time,mean_local_disk_space from dataFrame where startTime=%s"%(startTimeArr))

    schema_df = ["startTime","numberOfJob"]
    sumCPUUsage.toPandas().to_csv('thangbk2209/resource-startTime/%s.csv'%(startTimeArr[i]/1000000), index=False, header=None)

sc.stop()