# tinh resoure usage cua tung timeStamp - cong lai tong resouce usage trong tung file - moi file chua mot diem thoi gian
from pyspark.sql.session import SparkSession as spark
import pandas as pd
from pyspark import SparkContext
# from pyspark.sql.functions import avg
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from matplotlib import pyplot
import os
sc = SparkContext(appName="Task_usage")
sql_context = SQLContext(sc)

# folder_path ='/mnt/volume/ggcluster/clusterdata-2011-2/task_usage/'

dataSchema = StructType([StructField('value1', IntegerType(), True),
                         StructField('value2', IntegerType(), True)])


df = (
    sql_context.read
    .format('com.databricks.spark.csv')
    .schema(dataSchema)
    .load("google_cluster_analysis/test.csv")
)

df.createOrReplaceTempView("dataFrame")

testDf = sql_context.sql("SELECT avg(value1), avg(value2) from dataFrame ")

testDf.toPandas().to_csv('google_cluster_analysis/testAverage.csv', index=False, header=None)
sc.stop()