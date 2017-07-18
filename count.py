import pandas as pd
import os

folder_path = '/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/TopJobId/'
s=0;
for num in range(175,270):
	file_name = "JobMaxTaskpart-00"+str(num).zfill(3)+"-of-00500.csv"
	startTimeDf = pd.read_csv('%s%s'%(folder_path,file_name), header=None)
	# split dataset
	startTimeArr = startTimeDf.values
	s+=len(startTimeArr)
print s