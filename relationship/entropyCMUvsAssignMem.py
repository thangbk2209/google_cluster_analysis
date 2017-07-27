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

print symmetrical_uncertainly(meanCPUUsage,CMU)
print symmetrical_uncertainly(meanCPUUsage,AssignMem)
print symmetrical_uncertainly(meanCPUUsage,unmap_cache)
# print len(meanCPUUsage)
# print meanCPUUsage
# print meanCPUUsage.count(meanCPUUsage[0])
# print len(AssignMem)
# print len(CMU)
# print len(unmap_cache)
# print len(page_cache)
# print len(mean_disk)
# newDf=[]
# # print'lol'
# # print entro(meanCPUUsage)
# # print entro(CMU)
# # print entro(AssignMem)
# # print entroXY(CMU,mean_disk)
# newDf.append(meanCPUUsage)
# newDf.append(CMU)
# newDf.append(AssignMem)
# newDf.append(unmap_cache)
# newDf.append(page_cache)
# newDf.append(mean_disk)
# for i in range(len(newDf)):
# 	for j in range(i+1,len(newDf)):
# 		print colnames[i]+ '   '+ colnames[j] 
# 		print symmetrical_uncertainly(df[colnames[i]].values,df[colnames[j]].values)
		# print entropy(newDf[i],newDf[j]).sum()
# print entropy(meanCPUUsage, AssignMem)
# n, bins, patches = plt.hist(newDf[0],newDf[1])

# print n,bins,patches

# print df[colnames[0]].values
# print len(newDf)
# test = entropy(CMU)
# test1 = entropy(meanCPUUsage)
# test2 = entropy(AssignMem)
# a = entropy(CMU,mean_disk )
# b = entropy(meanCPUUsage,mean_disk)
# c = entropy(AssignMem,mean_disk)
# d = entropy(meanCPUUsage,AssignMem)
# print test
# print test1
# print test2
# print a
# print b
# print c
# print d
# print test - a
# print test1 - b
# print test2 - c