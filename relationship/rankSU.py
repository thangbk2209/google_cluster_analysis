import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from pandas import read_csv
import numpy as np
from pandas.plotting import autocorrelation_plot
from pandas.plotting import lag_plot
from matplotlib.backends.backend_pdf import PdfPages
import plotly.plotly as py
import seaborn as sns

colnames=['numberOfTaskIndex','numberOfMachineId','meanCPUUsage','CMU','AssignMem','unmapped_cache_usage','page_cache_usage', 'max_mem_usage','mean_diskIO_time','mean_local_disk_space','max_cpu_usage', 'max_disk_io_time', 'cpi', 'mai','sampled_cpu_usage']
df = read_csv('/home/nguyen/spark-lab/spark-2.1.1-bin-hadoop2.7/google_cluster_analysis/relationship/results/all_symmetrical_uncertainly.csv', header=None, index_col=False)

ggclusterCorelation = df.values
# print a
f=open('results/rank_SU_ggCluster.txt', 'wa')
result = []
myCorr=[]

for i in range(len(ggclusterCorelation)-1):
	for j in range(i+1,len(ggclusterCorelation[i]),1):
		if(ggclusterCorelation[i][j] >= 0.8):
			corr = []
			corr.append(i)
			corr.append(j)
			corr.append(ggclusterCorelation[i][j])
			# myCouple = colnames[i] +"-"+ colnames[j] +':'+ str(ggclusterCorelation[i][j]) +'\n'
			myCorr.append(corr)	
			# f.write(myCouple)
			# result.append(myCouple)
# np.savetxt("rank_corelation_ggCluster.txt", result)
for i in range(len(myCorr)-1):
	for j in range(i+1,len(myCorr),1):
		if myCorr[i][2] < myCorr[j][2]:
			# swap(myCorr[i],myCorr[j])
			T = myCorr[i]
			myCorr[i] = myCorr[j]
			myCorr[j] =T
print myCorr
dataX = []
dataY=[]
y = []
for k in range(len(myCorr)):
	couple= colnames[myCorr[k][0]] +"-"+ colnames[myCorr[k][1]]
	dataX.append(couple)
	dataY.append(myCorr[k][2])
	y.append(myCorr[k][2])
width = 1/1.5
x= range(len(dataX))
plt.bar(x,dataY, width, color="blue")
# plt.xticks(x, dataX)
# plt.plot(data)
# plt.show()
fig = plt.gcf()
pp = PdfPages('results/lol.pdf')
pp.savefig()
pp.close()
# plot_url = py.plot_mpl(fig, filename='mpl-basic-bar')

for k in range(len(myCorr)):
	myCouple = colnames[myCorr[k][0]] +"-"+ colnames[myCorr[k][1]] +':'+ str(myCorr[k][2]) +'\n'
	f.write(myCouple)
print result

