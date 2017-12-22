# Lay ra resource usage tai cac diem thoi gian- chi lay ra cac ban ghi thoa man rang start time < time stamp
# va end time > time stamp ma chua cong lai cac gia tri resource cua no
# y tuong: Tim ra min start time vaf max end time trong tung part. Sau do truy van du lieu trong tung part 
#voi gia tri tang dan tu gia tri min start time cua part do- tuy nhien chua xu li van de chong lan thoi gian
# giua cac part -- max end time cua part truoc > min start cuar part sau
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
folder_path = '/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/resoure_6176858948/'

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

schema_Timedf = ["startTime","endTime"]    
TimeValues = pd.read_csv('thangbk2209/plotTimeSeries/results/minMaxTimePart_resoure_6176858948.csv',names=schema_Timedf).values
TimeData = []
for i in range(len(TimeValues)):
    a=[]
    a.append(int(TimeValues[i][0]))
    a.append(int(TimeValues[i][1]))
    TimeData.append(a)
print TimeData
timeStart = TimeData[0][0]
timeNow = timeStart
timeEnd = TimeData[len(TimeData)-1][1]
numberOfPart = 0 # Dem so luong part khong rong da doc qua
# partNumber = 0  # vi tri part
extraTime = 600
# for file_name in os.listdir(folder_path):  
for partNumber in range(0,500):
    # f = open("TimeJobMaxTaskpart-00"+str(num).zfill(3)+"-of-00500.csv")
    
    file_name = "part-00"+str(partNumber).zfill(3)+"-of-00500.csv"
    if os.stat("%s%s"%(folder_path,file_name)).st_size != 0: 
        timeStartPart = TimeData[numberOfPart][0]
        timeEndPart = TimeData[numberOfPart][1]
        df = (
            sql_context.read
            .format('com.databricks.spark.csv')
            .schema(dataSchema)
            .load("%s%s"%(folder_path,file_name))
        )
        df.createOrReplaceTempView("dataFrame")
        numberOfPart +=1
        if numberOfPart != len(TimeData):
            next_file_name = "part-00"+str(partNumber+1).zfill(3)+"-of-00500.csv"
            timeCheck = TimeData[numberOfPart][0]  # Kiem tra xem phan thoi gian bat dau cua part tiep theo
                                        # voi thoi diem ket thuc part hien tai co bi chong lan khong
            if timeCheck <= timeEndPart:
                nextDf = (
                    sql_context.read
                    .format('com.databricks.spark.csv')
                    .schema(dataSchema)
                    .load("%s%s"%(folder_path,file_name))
                )
                nextDf.createOrReplaceTempView("nextDataFrame")

                for timeStamp in range(timeNow,timeEnd, extraTime):
                    if timeStamp >= timeEndPart:
                        timeNow = timeStamp
                        break
                    elif timeStamp < timeCheck:
                        resourceData = sql_context.sql("SELECT * from dataFrame where startTime <= %s and endTime > %s"%(timeStamp,timeStamp) )
                        resourceData.toPandas().to_csv('thangbk2209/tenMinutes_6176858948/%s-%s.csv'%(partNumber,timeStamp), index=False, header=None)
                    elif timeStamp >= timeCheck and timeStamp < timeEndPart:
                        resourceData1 = sql_context.sql("SELECT * from dataFrame where startTime <= %s and endTime > %s"%(timeStamp,timeStamp) )
                        resourceData1.toPandas().to_csv('thangbk2209/tenMinutes_6176858948/%s-%s.csv'%(partNumber,timeStamp), index=False, header=None)

                        resourceData2 = sql_context.sql("SELECT * from nextDataFrame where startTime <= %s and endTime > %s"%(timeStamp,timeStamp) )
                        resourceData2.toPandas().to_csv('thangbk2209/tenMinutes_6176858948/%s-%s.csv'%(partNumber+1,timeStamp), index=False, header=None)
            else:
                for timeStamp in range(timeNow,timeEnd, extraTime):
                    if timeStamp >= timeEndPart:
                        timeNow = timeStamp
                        break
                    else:
                        resourceData = sql_context.sql("SELECT * from dataFrame where startTime <= %s and endTime > %s"%(timeStamp,timeStamp) )
                        resourceData.toPandas().to_csv('thangbk2209/tenMinutes_6176858948/%s-%s.csv'%(partNumber,timeStamp), index=False, header=None)

        else:
            for timeStamp in range(timeNow,timeEnd, extraTime):
                resourceData = sql_context.sql("SELECT * from dataFrame where startTime <= %s and endTime > %s"%(timeStamp,timeStamp) )
                resourceData.toPandas().to_csv('thangbk2209/tenMinutes_6176858948/%s-%s.csv'%(partNumber,timeStamp), index=False, header=None)
          
sc.stop()