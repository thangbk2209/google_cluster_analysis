import pandas as pd
import numpy as np
import os
from pandas import read_csv
folder_path = '/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/resourceTopJopId/'

timeSeriesData=[]
for file_name in os.listdir(folder_path):
	print file_name
	df = read_csv('%s%s'%(folder_path,file_name), header=None,index_col=False)
	file_name_data = file_name.split('-')
	timeStampData = file_name_data[1].split('.')
	timeStamp = timeStampData[0]

	data = df.values
	newData = np.append(data[0],timeStamp)
	# newData = data[0]
	# newData.append(timeStamp)
	timeSeriesData.append(newData)
	print timeSeriesData
newDf = pd.DataFrame(timeSeriesData)
newDf.to_csv('/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/plotTimeSeries/sample_resource_usage_TopJobId.csv')