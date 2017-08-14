import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from pandas import read_csv
import numpy as np
from pandas.plotting import autocorrelation_plot
from pandas.plotting import lag_plot
from matplotlib.backends.backend_pdf import PdfPages
import seaborn as sns
import pandas as pd

colnames=['numberOfTaskIndex','numberOfMachineId','meanCPUUsage','CMU','AssignMem','unmapped_cache_usage','page_cache_usage', 'max_mem_usage','mean_diskIO_time','mean_local_disk_space','max_cpu_usage', 'max_disk_io_time', 'cpi', 'mai','sampled_cpu_usage']
I = pd.Index(['numberOfTaskIndex','numberOfMachineId','meanCPUUsage','CMU','AssignMem','unmapped_cache_usage','page_cache_usage', 'max_mem_usage','mean_diskIO_time','mean_local_disk_space','max_cpu_usage', 'max_disk_io_time', 'cpi', 'mai','sampled_cpu_usage'], name="rows")
mldf = read_csv('/home/nguyen/spark-lab/spark-2.1.1-bin-hadoop2.7/google_cluster_analysis/relationship/results/all_symmetrical_uncertainly.csv', header=None, index_col=False, names =colnames)
df = pd.DataFrame(mldf.values, index=I, columns=colnames)
print df
# su = df.values
# print su
# corelationMatrix = df.corr()
# corr = df.corr()
# sns.heatmap(corelationMatrix, mask=np.zeros_like(corelationMatrix, dtype=np.bool), cmap=sns.diverging_palette(220, 10, as_cmap=True),
#             square=True, ax=ax)
mask = np.zeros_like(df, dtype=np.bool)
mask[np.triu_indices_from(mask)] = True

f, ax = plt.subplots(figsize=(11, 9))
cmap = sns.diverging_palette(230, 20, as_cmap=True)

sns.heatmap(df, mask=mask, cmap=cmap, vmax=1.0, center=0,
            square=True, linewidths=.5, cbar_kws={"shrink": .5})

pp = PdfPages('results/plotSU.pdf')
pp.savefig()
# plt.savefig('results/predictUnmap_cache-page_cache.png')
pp.close()
# plt.show()
# corr.to_csv('results/AllCorelationMatrix.csv')

# print corelationMatrix
