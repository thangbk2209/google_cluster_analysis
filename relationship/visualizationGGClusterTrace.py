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
# Counter(array1.most_common(1))
# print math.log(2,2)

# a =[1,2,3,4,5,1,2]
# print Counter(a.most_common(1))

def entro(X):
	x = [] # luu lai danh sach cac gia tri X[i] da tinh
	tong_so_lan = 0
	result = 0
	p=[]
	for i in range(len(X)):
		if Counter(x)[X[i]]==0:
			so_lan = Counter(X)[X[i]]
			tong_so_lan += so_lan
			x.append(X[i])
			P = 1.0*so_lan / len(X)
			p.append(P)
			result -= P * math.log(P,2)
		if tong_so_lan == len(X):
			break
	return result
def entroXY(X,Y):
	y = []
	result = 0
	pY = []
	tong_so_lan_Y = 0
	for i in range(len(Y)):
		# print Counter(y)[Y[i]]
		if Counter(y)[Y[i]]==0:
			x=[]
			so_lan_Y = Counter(Y)[Y[i]]
			tong_so_lan_Y += so_lan_Y
			y.append(Y[i])
			PY = 1.0* so_lan_Y / len(Y)
			# vi_tri = Y.index(Y[i])
			vi_tri=[]
			for k in range(len(Y)):
				if Y[k] == Y[i]: 
					vi_tri.append(k)
			for j in range(len(vi_tri)):
				x.append(X[vi_tri[j]])
			entro_thanh_phan = entro(x)
			result += PY * entro_thanh_phan
		if tong_so_lan_Y == len(Y):
			break
	return result
def infomation_gain(X,Y):
	return entro(X) - entroXY(X,Y)
def symmetrical_uncertainly(X,Y):
	return 2.0*infomation_gain(X,Y)/(entro(X)+entro(Y))


# colnames=['meanCPUUsage' ,'CMU' ,'AssignMem' ,'unmap_page_cache_memory_ussage' ,'page_cache_usage' ,'mean_local_disk_space', 'timeStamp']
# df = read_csv('/home/nguyen/spark-lab/spark-2.1.1-bin-hadoop2.7/google_cluster_analysis/results/my_offical_data_resource_TopJobId.csv', header=None, index_col=False, names=colnames)
colnames=['time_stamp','numberOfTaskIndex','numberOfMachineId','meanCPUUsage','CMU','AssignMem','unmapped_cache_usage','page_cache_usage', 'max_mem_usage','mean_diskIO_time','mean_local_disk_space','max_cpu_usage', 'max_disk_io_time', 'cpi', 'mai','sampled_cpu_usage']
df = read_csv('/home/nguyen/spark-lab/spark-2.1.1-bin-hadoop2.7/google_cluster_analysis/results/data_resource_JobId_6336594489.csv', header=None, index_col=False, names=colnames)
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
for i in range(47505):
	time_stamp.append(df['time_stamp'])
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
	ax0.set(title="Visualization", xlabel=colnames[0], ylabel=colnames[j])
	plt.savefig('results/%s.png'%(colnames[j]))
	pp.close()






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

