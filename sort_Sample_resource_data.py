
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
folder_path = '/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/plotTimeSeries/'

dataSchema = StructType([
                         # StructField('JobId', LongType(), True),
                         StructField('meanCPUUsage', FloatType(), True),
                         # canonical memory usage
                         StructField('CMU', FloatType(), True),
                         # assigned memory usage
                         StructField('AssignMem', FloatType(), True),
                         # unmapped page cache memory usage
                         StructField('unmapped_cache_usage', FloatType(), True),
                         StructField('page_cache_usage', FloatType(), True),
                         StructField('mean_local_disk_space', FloatType(), True),
                         StructField('time_stamp', LongType(), True)])
file_name = "offical_data_resource_TopJobId.csv"
df = (
    sql_context.read
    .format('com.databricks.spark.csv')
    .schema(dataSchema)
    .load("%s%s"%(folder_path,file_name))
)
df.createOrReplaceTempView("dataFrame")

DataDf = sql_context.sql("SELECT  * from dataFrame order by time_stamp ASC")
print "DataDf.count()= "
print DataDf.count()
DataDf.toPandas().to_csv('thangbk2209/plotTimeSeries/my_offical_data_resource_TopJobId.csv', index=False, header=None)
sc.stop()