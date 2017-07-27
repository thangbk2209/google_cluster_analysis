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

fig = plt.figure(figsize=(10,20))
# print AssignMem
ax0 = plt.subplot2grid((5,1),(0,0))
ax0.scatter(CMU,AssignMem )

ax1 = plt.subplot2grid((5,1),(1,0))
ax1.scatter(CMU,meanCPUUsage )

ax2 = plt.subplot2grid((5,1),(2,0))
ax2.scatter(CMU,unmap_cache)

ax3 = plt.subplot2grid((5,1),(3,0))
ax3.scatter(CMU,page_cache)

ax4 = plt.subplot2grid((5,1),(4,0))
ax4.scatter(CMU,mean_disk)
plt.ylim((0,2))
# ax5 = plt.subplot2grid((5,1),(5,0))
# ax5.scatter(CMU,AssignMem )
# linear = LR()
# linear.fit(CMU,AssignMem)
# print linear.fit_intercept
ax0.set(title="Linear Corelation", xlabel="CMU", ylabel="AssignMem")
# plt.show()
# print corelationMatrix
axes = plt.gca()
print 'axes'
print axes.get_xlim()[0],axes.get_xlim()[1]
m, b = np.polyfit(CMU, AssignMem, 1)

print m
print b

X_plot = np.linspace(axes.get_xlim()[0],axes.get_xlim()[1],100)

print X_plot
ax0.plot(X_plot, m*X_plot + b,color='r')

p,q = np.polyfit(CMU, mean_disk, 1)
print 'p,q: '
print p
print q
Q_plot = np.linspace(axes.get_xlim()[0],axes.get_xlim()[1],100)
ax4.plot(Q_plot, p*Q_plot + q,color='r')

pp = PdfPages('results/CMU-AssignMem.pdf')
pp.savefig()
pp.close()
