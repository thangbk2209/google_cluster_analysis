import pandas as pd
import numpy as np
import os
from pandas import read_csv
folder_path = '/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/resourceTopJopId/'
name=[]
for file_name in os.listdir(folder_path):
	print file_name
	try:
		df = read_csv('%s%s'%(folder_path,file_name), header=None,index_col=False)
	except EmptyDataError:
		name.append(file_name)
print name