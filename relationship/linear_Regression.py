import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from pandas import read_csv
from pandas.plotting import autocorrelation_plot
from pandas.plotting import lag_plot
from matplotlib.backends.backend_pdf import PdfPages
from sklearn import datasets, linear_model
import numpy as np
from sklearn.metrics import mean_squared_error
# def mean_absolute_percentage_error(y_true, y_pred): 
#     # y_true, y_pred = check_arrays(y_true, y_pred)

#     ## Note: does not handle mix 1d representation
#     #if _is_1d(y_true): 
#     #    y_true, y_pred = _check_1d_array(y_true, y_pred)

#     return np.mean(np.abs((y_true - y_pred) / y_true)) * 100


colnames=['meanCPUUsage' ,'CMU' ,'AssignMem' ,'unmap_page_cache_memory_ussage' ,'page_cache_usage' ,'mean_local_disk_space', 'timeStamp']
df = read_csv('/home/nguyen/spark-lab/spark-2.1.1-bin-hadoop2.7/google_cluster_analysis/results/my_offical_data_resource_TopJobId.csv', header=None, index_col=False, names=colnames)
meanCPUUsage = df['meanCPUUsage'].values
AssignMem =df['AssignMem'].values
CMU = df['CMU'].values
unmap_cache = df['unmap_page_cache_memory_ussage'].values
page_cache = df['page_cache_usage'].values
mean_disk = df['mean_local_disk_space'].values
# print len(meanCPUUsage)
# print len(unmap_cache)
fig = plt.figure(figsize=(10,30))
# print AssignMem
ax0 = plt.subplot2grid((1,1),(0,0))

train_size = int(len(meanCPUUsage) * 0.85)
# trainCPUUsage, testCPUUsage = meanCPUUsage[1:train_size], meanCPUUsage[train_size:]
# trainUnmap_cache, testUnmap_cache = unmap_cache[1:train_size], unmap_cache[train_size:]
trainCPUUsage = []
testCPUUsage = []
trainUnmap_cache = []
testUnmap_cache = []
for i in range(len(meanCPUUsage)):
	if i <= train_size:
		trainCPUUsage.append(meanCPUUsage[i])
		trainUnmap_cache.append(unmap_cache[i])
	else:
		testCPUUsage.append(meanCPUUsage[i])
		testUnmap_cache.append(unmap_cache[i])
# print train_size
regr = linear_model.LinearRegression()
regr.fit(np.transpose(np.matrix(trainCPUUsage)), np.transpose(np.matrix(trainUnmap_cache)))
# regr.fit(trainCPUUsage,trainUnmap_cache)
print('Coefficients: \n', regr.coef_)
print regr.get_params()
print testCPUUsage
predicts =  regr.predict(np.transpose(np.matrix(testCPUUsage)))

print 'predicts: '
print predicts
predictsArr = []
for i in range(len(predicts)):
	predictsArr.append(predicts[i][0])
mean_square_error = mean_squared_error(testCPUUsage, predictsArr)
print mean_square_error
print len(predictsArr)
print predictsArr[0],predictsArr[1]
print testUnmap_cache[0],testUnmap_cache[1]
ax0.scatter(trainCPUUsage,trainUnmap_cache, color="black")
ax0.scatter(testCPUUsage, testUnmap_cache,  color='black')
ax0.plot(testCPUUsage ,predictsArr, color='blue', linewidth=3)
ax0.text(40, 0, 'mean_squared_error:%s'%(mean_square_error), style='italic',
        bbox={'facecolor':'red', 'alpha':0.5, 'pad':10})
# plt.xticks(())

# plt.yticks(())
pp = PdfPages('results/testCPUUsage-unmapCache.png')
pp.savefig()
pp.close()
# plt.show()