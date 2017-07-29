import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from pandas import read_csv
from pandas.plotting import autocorrelation_plot
from pandas.plotting import lag_plot
from matplotlib.backends.backend_pdf import PdfPages
from sklearn.linear_model import LinearRegression as LR
import numpy as np

colnames=['meanCPUUsage' ,'CMU' ,'AssignMem' ,'unmap_page_cache_memory_ussage' ,'page_cache_usage' ,'mean_local_disk_space', 'timeStamp']
df = read_csv('/home/nguyen/spark-lab/spark-2.1.1-bin-hadoop2.7/google_cluster_analysis/results/my_offical_data_resource_TopJobId.csv', header=None, index_col=False, names=colnames)
meanCPUUsage = df['meanCPUUsage'].values
AssignMem =df['AssignMem'].values
CMU = df['CMU'].values
unmap_cache = df['unmap_page_cache_memory_ussage'].values
page_cache = df['page_cache_usage'].values
mean_disk = df['mean_local_disk_space'].values

fig = plt.figure(figsize=(10,30))
# print AssignMem
ax0 = plt.subplot2grid((5,1),(0,0))
ax0.scatter(unmap_cache,page_cache)
ax0.set(title="Linear Corelation", xlabel="unmap_page_cache_memory_ussage", ylabel="page_cache_usage")
# plt.show()
# print corelationMatrix
axes = plt.gca()
print 'axes'
print axes.get_xlim()[0],axes.get_xlim()[1]
m, b = np.polyfit(unmap_cache,page_cache, 1)

print m
print b
# plt.text('m,b:%s%s'%(m,b))
ax0.text(axes.get_xlim()[1]/2, 0, 'm:%s ,b:%s'%(m,b), style='italic',
        bbox={'facecolor':'red', 'alpha':0.5, 'pad':10})
X_plot = np.linspace(axes.get_xlim()[0],axes.get_xlim()[1],100)

print X_plot
ax0.plot(X_plot, m*X_plot + b,color='r')


pp = PdfPages('results/unmap_page_cache_memory_ussage-page_cache_usage.pdf')
pp.savefig()
pp.close()
