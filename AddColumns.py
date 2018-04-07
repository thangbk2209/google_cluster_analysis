# Tien hanh them cot nhan thoi gian, cung voi do noi tat ca cac diem thoi gian lai va dua ra file csv
import pandas as pd
import numpy as np
import os
from pandas import read_csv
folder_path = 'thangbk2209/resource_tenMinutes_6176858948/'

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
		oldData=[6176858948, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
		         # 6336594489,1 , 1, 0, 0, 0, 0, 0, 0,  ,0 ,0 ,  ,  ,  ,0 ,0,0.0

		newData = np.append(oldData,timeStamp)
		timeSeriesData.append(newData)
		
newDf = pd.DataFrame(timeSeriesData)
# df1 = newDf.replace(np.nan, 0, regex=True)
newDf.to_csv('thangbk2209/google_cluster_analysis/results/resource_usage_tenMinutes_6176858948.csv', index=False, header=None)