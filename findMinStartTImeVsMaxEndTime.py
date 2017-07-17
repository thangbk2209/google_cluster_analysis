
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
folder_path = '/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/TopJobId/'

dataSchema = StructType([StructField('startTime', StringType(), True),
                         StructField('endTime', StringType(), True),
                         StructField('JobId', LongType(), True),
                         StructField('meanCPUUsage', FloatType(), True),
                         # canonical memory usage
                         StructField('CMU', FloatType(), True),
                         # assigned memory usage
                         StructField('AssignMem', FloatType(), True),
                         # unmapped page cache memory usage
                         StructField('unmapped_cache_usage', FloatType(), True),
                         StructField('page_cache_usage', FloatType(), True),
                         StructField('mean_diskIO_time', FloatType(), True),
                         StructField('mean_local_disk_space', FloatType(), True)])

for for num in range(175,270):
    file_name = "JobMaxTaskpart-00"+str(num).zfill(3)+"-of-00500.csv"
    df = (
        sql_context.read
        .format('com.databricks.spark.csv')
        .schema(dataSchema)
        .load("%s%s"%(folder_path,file_name))
    )
    df.createOrReplaceTempView("dataFrame")
   
    TimeDf = sql_context.sql("SELECT min(startTime),max(endTime) from dataFrame")
   
    TimeDf.toPandas().to_csv('thangbk2209/minMaxTopJobId/Time%s'%(file_name), index=False, header=None)
    schema_Timedf = ["startTime","endTime"]
   
    TimeData = pd.read_csv('thangbk2209/minMaxTopJobId/Time%s'%(file_name),names=schema_Timedf)

    minStartTime=TimeData['startTime']
    maxEndTime = TimeData['endTime']
    extraTime = 10
    for i in range(minStartTime,maxEndTime, extraTime):
        newData = sql_context.sql("SELECT * from dataFrame where startTime <= %s and endTime > %s"%(i,i) )
        # newData.withColumn('timeStamp',i)
        newData.toPandas().to_csv('thangbk2209/tenSecondsTopJobId/%s.csv'%(i), index=False, header=None)
    # df.printSchema()
    
    # sumCPUUsage.write.save("results/test.csv", format="csv", columns=schema_df)
sc.stop()