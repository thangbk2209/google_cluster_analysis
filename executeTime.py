
from pyspark.sql.session import SparkSession as spark
import pandas as pd
from pandas import read_csv
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
minMaxTimeDf = read_csv('/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/minMaxTopJobId/minMaxTimePart.csv', header=None, index_col=False)
minMaxTimeArr = minMaxTimeDf.values  # Lay ra cac gia tri min max time doi voi job id do trong tung part.
                                     # Gia tri dau tien la min start time va gia tri thu 2 la max end time.
print minMaxTimeArr
for file_name in os.listdir(folder_path):
    file_name_data = file_name.split('-')  #Phan tu dau tien cua mang la vi tri part
    part_number = int(file_name_data[0])
    
    timePointData = file_name[1].split('.')  
    timePoint = int(timePointData[0])            # Lay ra nhan thoi gian
    if(part_number == 175):
        print part_number
        print minMaxTimeArr[1][0]
        print int(minMaxTimeArr[1][0])+50
        if(timePoint > int(minMaxTimeArr[1][0]) and timePoint < int(minMaxTimeArr[0][1])):
            print timePoint
            part_name = "/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/TopJobId/JobMaxTaskpart-00176-of-00500.csv"
            df = (
                sql_context.read
                .format('com.databricks.spark.csv')
                .schema(dataSchema)
                .load("%s"%(part_name))
            )
            df.createOrReplaceTempView("dataFrame")
            newData = sql_context.sql("SELECT * from dataFrame where startTime <= %s and endTime > %s"%(timePoint,timePoint) )
            newData.toPandas().to_csv('thangbk2209/newTenSecondsTopJobId/176-%s.csv'%(timePoint), index=False, header=None)
    elif(part_number == 270):
        print part_number
        print minMaxTimeArr[1][0]
        print int(minMaxTimeArr[1][0])+50
        if(timePoint > minMaxTimeArr[95][0] and timePoint < minMaxTimeArr[94][1]):
            part_name = "/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/TopJobId/JobMaxTaskpart-00"+str(int(file_name_data[0])-1).zfill(3)+"-of-00500.csv"
            df = (
                sql_context.read
                .format('com.databricks.spark.csv')
                .schema(dataSchema)
                .load("%s"%(part_name))
            )
            df.createOrReplaceTempView("dataFrame")
            newData = sql_context.sql("SELECT * from dataFrame where startTime <= %s and endTime > %s"%(timePoint,timePoint) )
            newData.toPandas().to_csv('thangbk2209/newTenSecondsTopJobId/269-%s.csv'%(timePoint), index=False, header=None)
    else:
        print part_number
        print minMaxTimeArr[1][0]
        print int(minMaxTimeArr[1][0])+50
        if(timePoint < minMaxTimeArr[part_number-175][1] and timePoint > minMaxTimeArr[part_number-174][0]):
            part_name = "/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/TopJobId/JobMaxTaskpart-00"+str(part_number+1).zfill(3)+"-of-00500.csv"
            df = (
                sql_context.read
                .format('com.databricks.spark.csv')
                .schema(dataSchema)
                .load("%s"%(part_name))
            )
            df.createOrReplaceTempView("dataFrame")
            newData = sql_context.sql("SELECT * from dataFrame where startTime <= %s and endTime > %s"%(timePoint,timePoint) )
            newData.toPandas().to_csv('thangbk2209/newTenSecondsTopJobId/%s-%s.csv'%(part_number+1,timePoint), index=False, header=None)
        if(timePoint > minMaxTimeArr[part_number-175][0] and timePoint < minMaxTimeArr[part_number-176][1]):
            part_name = "/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/TopJobId/JobMaxTaskpart-00"+str(part_number-1).zfill(3)+"-of-00500.csv"
            df = (
                sql_context.read
                .format('com.databricks.spark.csv')
                .schema(dataSchema)
                .load("%s"%(part_name))
            )
            df.createOrReplaceTempView("dataFrame")
            newData = sql_context.sql("SELECT * from dataFrame where startTime <= %s and endTime > %s"%(timePoint,timePoint) )
            newData.toPandas().to_csv('thangbk2209/newTenSecondsTopJobId/%s-%s.csv'%(part_number-1,timePoint), index=False, header=None)
sc.stop()