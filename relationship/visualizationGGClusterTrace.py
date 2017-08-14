import matplotlib as mpl
mpl.use('Agg')
import math
import matplotlib.pyplot as plt
from pandas import read_csv
from pandas.plotting import autocorrelation_plot
from pandas.plotting import lag_plot
from matplotlib.backends.backend_pdf import PdfPages
from sklearn.linear_model import LinearRegression as LR
import numpy as np
import pandas as pd
from scipy.stats import entropy

from collections import Counter


# colnames=['meanCPUUsage' ,'CMU' ,'AssignMem' ,'unmap_page_cache_memory_ussage' ,'page_cache_usage' ,'mean_local_disk_space', 'timeStamp']
# df = read_csv('/home/nguyen/spark-lab/spark-2.1.1-bin-hadoop2.7/google_cluster_analysis/results/my_offical_data_resource_TopJobId.csv', header=None, index_col=False, names=colnames)
colnames=['time_stamp','numberOfTaskIndex','numberOfMachineId','meanCPUUsage','CMU','AssignMem','unmapped_cache_usage','page_cache_usage', 'max_mem_usage','mean_diskIO_time','mean_local_disk_space','max_cpu_usage', 'max_disk_io_time', 'cpi', 'mai','sampled_cpu_usage']
df = read_csv('/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/plotTimeSeries/results/data_resource_JobId_6336594489.csv', header=None, index_col=False, names=colnames)
time_stamp=[]
numberOfTaskIndex = df['numberOfTaskIndex'].values

numberOfMachineId = df['numberOfMachineId'].values
meanCPUUsage = []
CMU = []
AssignMem = []
unmapped_cache_usage = []
page_cache_usage = []
max_mem_usage = []
mean_diskIO_time = []
mean_local_disk_space = []
max_cpu_usage = []
max_disk_io_time = []
cpi = []
mai = []
sampled_cpu_usage=[]
time_stamp.append(df['time_stamp'])
for i in range(47505):
	
	meanCPUUsage.append(round(df['meanCPUUsage'].values[i],2))
	CMU.append(round(df['CMU'].values[i],2))
	AssignMem.append(round(df['AssignMem'].values[i],2))
	unmapped_cache_usage.append(round(df['unmapped_cache_usage'].values[i],2))
	page_cache_usage.append(round(df['page_cache_usage'].values[i],2))
	max_mem_usage.append(round(df['max_mem_usage'].values[i],2))
	mean_diskIO_time.append(round(df['mean_diskIO_time'].values[i],2))
	mean_local_disk_space.append(round(df['mean_local_disk_space'].values[i],2))
	max_cpu_usage.append(round(df['max_cpu_usage'].values[i],2))
	max_disk_io_time.append(round(df['max_disk_io_time'].values[i],2))
	cpi.append(round(df['cpi'].values[i],2))
	mai.append(round(df['mai'].values[i],2))
	sampled_cpu_usage.append(round(df['sampled_cpu_usage'].values[i],2))
newDf = []
# newDf.append(time_stamp)
newDf.append(numberOfTaskIndex)
newDf.append(numberOfMachineId)
newDf.append(meanCPUUsage)
newDf.append(CMU)
newDf.append(AssignMem)
newDf.append(unmapped_cache_usage)
newDf.append(page_cache_usage)
newDf.append(max_mem_usage)
newDf.append(mean_diskIO_time)
newDf.append(mean_local_disk_space)
newDf.append(max_cpu_usage)
newDf.append(max_disk_io_time)
newDf.append(cpi)
newDf.append(mai)
newDf.append(sampled_cpu_usage)
test=[]
# for i in range(len(meanCPUUsage)):
# 	test.append(meanCPUUsage.count(meanCPUUsage[i]))
# print test
su=[]
for j in range(len(newDf)):
	fig = plt.figure(figsize=(10,20))
	# print AssignMem
	ax0 = plt.subplot2grid((1,1),(0,0))
	ax0.scatter(time_stamp,newDf[j])
	ax0.set(title="Visualization", xlabel=colnames[0], ylabel=colnames[j+1])
	plt.savefig('results/%s.png'%(colnames[j+1]))






# 	print 'j ',j
# 	suj=[]
# 	for k in range(j+1,len(newDf),1):
# 		print 'k ',k
# 		suj.append(symmetrical_uncertainly(newDf[j],newDf[k]))
# 	print suj
# 	su.append(suj)
# print su
# suDf = pd.DataFrame(su)
# suDf.to_csv("symmetrical_uncertainly.csv")
# np.savetxt("foo.csv", su, delimiter=",")

