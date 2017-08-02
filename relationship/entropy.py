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
df = read_csv('/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/plotTimeSeries/results/data_resource_JobId_6336594489.csv', header=None, index_col=False, names=colnames)

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
for j in range(len(newDf)-1):
	suj=[]
	for k in range(j+1,len(newDf),1):
		suj.append(symmetrical_uncertainly(newDf[j],newDf[k]))
	su.append(suj)
np.savetxt("foo.csv", su, delimiter=",")
# print entro(newDf[1])
# print symmetrical_uncertainly(newDf[1], newDf[2])



# f=open('results/test.txt', 'wa')
# f.write('meanCPUUsage,CMU: '+str(symmetrical_uncertainly(meanCPUUsage,CMU)))
# f.write('meanCPUUsage,AssignMem: '+str(symmetrical_uncertainly(meanCPUUsage,AssignMem)))
# f.write('meanCPUUsage,unmap_cache: '+str(symmetrical_uncertainly(meanCPUUsage,unmap_cache)))
# f.write('meanCPUUsage,page_cache: '+str(symmetrical_uncertainly(meanCPUUsage,page_cache)))
# f.write('meanCPUUsage,mean_disk: '+str(symmetrical_uncertainly(meanCPUUsage,mean_disk)))
# f.write('CMU,meanCPUUsage: '+str(symmetrical_uncertainly(CMU,meanCPUUsage)))
# f.write('CMU,AssignMem: '+str(symmetrical_uncertainly(CMU,AssignMem)))
# f.write('CMU,unmap_cache: '+str(symmetrical_uncertainly(CMU,unmap_cache)))
# f.write('CMU,unmap_cache: '+str(symmetrical_uncertainly(CMU,page_cache)))
# f.write('CMU,mean_disk: '+str(symmetrical_uncertainly(CMU,mean_disk)))
# f.write('AssignMem,meanCPUUsage: '+str(symmetrical_uncertainly(AssignMem,meanCPUUsage)))
# f.write('AssignMem,CMU: '+str(symmetrical_uncertainly(AssignMem,CMU)))
# f.write('AssignMem,unmap_cache: '+str(symmetrical_uncertainly(AssignMem,unmap_cache)))
# f.write('AssignMem,page_cache: '+str(symmetrical_uncertainly(AssignMem,page_cache)))
# f.write('AssignMem,mean_disk: '+str(symmetrical_uncertainly(AssignMem,mean_disk)))
# f.write('unmap_cache,meanCPUUsage: '+str(symmetrical_uncertainly(unmap_cache,meanCPUUsage)))
# f.write('unmap_cache,CMU: '+str(symmetrical_uncertainly(unmap_cache,CMU)))
# f.write('unmap_cache,AssignMem: '+str(symmetrical_uncertainly(unmap_cache,AssignMem)))
# f.write('unmap_cache,page_cache: '+str(symmetrical_uncertainly(unmap_cache,page_cache)))
# f.write('unmap_cache,mean_disk: '+str(symmetrical_uncertainly(unmap_cache,mean_disk)))
# f.write('page_cache,meanCPUUsage: '+str(symmetrical_uncertainly(page_cache,meanCPUUsage)))
# f.write('page_cache,CMU: '+str(symmetrical_uncertainly(page_cache,CMU)))
# f.write('page_cache,AssignMem: '+str(symmetrical_uncertainly(page_cache,AssignMem)))
# f.write('page_cache,unmap_cache: '+str(symmetrical_uncertainly(page_cache,unmap_cache)))
# f.write('page_cache,mean_disk: '+str(symmetrical_uncertainly(page_cache,mean_disk)))
# f.write('mean_disk,meanCPUUsage: '+str(symmetrical_uncertainly(mean_disk,meanCPUUsage)))
# f.write('mean_disk,CMU: '+str(symmetrical_uncertainly(mean_disk,CMU)))
# f.write('mean_disk,AssignMem: '+str(symmetrical_uncertainly(mean_disk,AssignMem)))
# f.write('mean_disk,unmap_cache: '+str(symmetrical_uncertainly(mean_disk,unmap_cache)))
# f.write('mean_disk,page_cache: '+str(symmetrical_uncertainly(mean_disk,page_cache)))

# print symmetrical_uncertainly(meanCPUUsage,AssignMem)
# print symmetrical_uncertainly(meanCPUUsage,unmap_cache)
