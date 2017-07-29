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

# print math.log(2,2)
def entro(X):
	x = []
	tong_so_lan = 0
	result = 0
	p=[]
	for i in range(len(X)):
		if x.count(X[i]) == 0:
			so_lan = X.count(X[i])
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
		if y.count(Y[i]) == 0:
			x=[]
			so_lan_Y = Y.count(Y[i])
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


colnames=['meanCPUUsage' ,'CMU' ,'AssignMem' ,'unmap_page_cache_memory_ussage' ,'page_cache_usage' ,'mean_local_disk_space', 'timeStamp']
df = read_csv('/home/nguyen/spark-lab/spark-2.1.1-bin-hadoop2.7/google_cluster_analysis/results/my_offical_data_resource_TopJobId.csv', header=None, index_col=False, names=colnames)
meanCPUUsage=[]
AssignMem =[]
CMU = []
unmap_cache = []
page_cache = []
mean_disk = []
for i in range(48308):
	meanCPUUsage.append(round(df['meanCPUUsage'].values[i],2))
	CMU.append(round(df['CMU'].values[i],2))
	AssignMem.append(round(df['AssignMem'].values[i],2))
	unmap_cache.append(round(df['unmap_page_cache_memory_ussage'].values[i],2))
	page_cache.append(round(df['page_cache_usage'].values[i],2))
	mean_disk.append(round(df['mean_local_disk_space'].values[i],2))

f=open('results/test.txt', 'wa')
f.write('meanCPUUsage,CMU: '+str(symmetrical_uncertainly(meanCPUUsage,CMU)))
f.write('meanCPUUsage,AssignMem: '+str(symmetrical_uncertainly(meanCPUUsage,AssignMem)))
f.write('meanCPUUsage,unmap_cache: '+str(symmetrical_uncertainly(meanCPUUsage,unmap_cache)))
f.write('meanCPUUsage,page_cache: '+str(symmetrical_uncertainly(meanCPUUsage,page_cache)))
f.write('meanCPUUsage,mean_disk: '+str(symmetrical_uncertainly(meanCPUUsage,mean_disk)))
f.write('CMU,meanCPUUsage: '+str(symmetrical_uncertainly(CMU,meanCPUUsage)))
f.write('CMU,AssignMem: '+str(symmetrical_uncertainly(CMU,AssignMem)))
f.write('CMU,unmap_cache: '+str(symmetrical_uncertainly(CMU,unmap_cache)))
f.write('CMU,unmap_cache: '+str(symmetrical_uncertainly(CMU,page_cache)))
f.write('CMU,mean_disk: '+str(symmetrical_uncertainly(CMU,mean_disk)))
f.write('AssignMem,meanCPUUsage: '+str(symmetrical_uncertainly(AssignMem,meanCPUUsage)))
f.write('AssignMem,CMU: '+str(symmetrical_uncertainly(AssignMem,CMU)))
f.write('AssignMem,unmap_cache: '+str(symmetrical_uncertainly(AssignMem,unmap_cache)))
f.write('AssignMem,page_cache: '+str(symmetrical_uncertainly(AssignMem,page_cache)))
f.write('AssignMem,mean_disk: '+str(symmetrical_uncertainly(AssignMem,mean_disk)))
f.write('unmap_cache,meanCPUUsage: '+str(symmetrical_uncertainly(unmap_cache,meanCPUUsage)))
f.write('unmap_cache,CMU: '+str(symmetrical_uncertainly(unmap_cache,CMU)))
f.write('unmap_cache,AssignMem: '+str(symmetrical_uncertainly(unmap_cache,AssignMem)))
f.write('unmap_cache,page_cache: '+str(symmetrical_uncertainly(unmap_cache,page_cache)))
f.write('unmap_cache,mean_disk: '+str(symmetrical_uncertainly(unmap_cache,mean_disk)))
f.write('page_cache,meanCPUUsage: '+str(symmetrical_uncertainly(page_cache,meanCPUUsage)))
f.write('page_cache,CMU: '+str(symmetrical_uncertainly(page_cache,CMU)))
f.write('page_cache,AssignMem: '+str(symmetrical_uncertainly(page_cache,AssignMem)))
f.write('page_cache,unmap_cache: '+str(symmetrical_uncertainly(page_cache,unmap_cache)))
f.write('page_cache,mean_disk: '+str(symmetrical_uncertainly(page_cache,mean_disk)))
f.write('mean_disk,meanCPUUsage: '+str(symmetrical_uncertainly(mean_disk,meanCPUUsage)))
f.write('mean_disk,CMU: '+str(symmetrical_uncertainly(mean_disk,CMU)))
f.write('mean_disk,AssignMem: '+str(symmetrical_uncertainly(mean_disk,AssignMem)))
f.write('mean_disk,unmap_cache: '+str(symmetrical_uncertainly(mean_disk,unmap_cache)))
f.write('mean_disk,page_cache: '+str(symmetrical_uncertainly(mean_disk,page_cache)))

# print symmetrical_uncertainly(meanCPUUsage,AssignMem)
# print symmetrical_uncertainly(meanCPUUsage,unmap_cache)
