# Tien hanh them cot nhan thoi gian, cung voi do noi tat ca cac diem thoi gian lai va dua ra file csv
import pandas as pd
import numpy as np
import os
from pandas import read_csv
folder_path = '/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/resourceTopJopId/'

timeSeriesData=[]
for file_name in os.listdir(folder_path):
	# print file_name
	file_name_data = file_name.split('-')
	timeStampData = file_name_data[1].split('.')
	timeStamp = timeStampData[0]
	try:
		df = read_csv('%s%s'%(folder_path,file_name), header=None,index_col=False)
		data = df.values
		newData = np.append(data[0],timeStamp)
		# newData = data[0]
		# newData.append(timeStamp)
		timeSeriesData.append(newData)
		# print timeSeriesData
		# print len(timeSeriesData)
	except pd.io.common.EmptyDataError:
		oldData=[6336594489, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
		         # 6336594489,1 , 1, 0, 0, 0, 0, 0, 0,  ,0 ,0 ,  ,  ,  ,0 ,0,0.0

		newData = np.append(oldData,timeStamp)
		timeSeriesData.append(newData)
		
newDf = pd.DataFrame(timeSeriesData)
# df1 = newDf.replace(np.nan, 0, regex=True)
newDf.to_csv('/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/plotTimeSeries/results/all_sample_resource_usage_TopJobId.csv', index=False, header=None)