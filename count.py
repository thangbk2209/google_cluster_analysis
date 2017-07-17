import pandas as pd
import os
folder_path='/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/TopJobId/'
for file_name in os.listdir(folder_path):
	startTimeDf = pd.read_csv('%s'%(file_name), header=None)
	# split dataset
	startTimeArr = startTimeDf.values
	print len(startTimeArr)