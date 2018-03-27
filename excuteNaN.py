import pandas as pd
import numpy as np
import os
from pandas import read_csv


df = read_csv('thangbk2209/google_cluster_analysis/results/resource_usage_fiveMinutes_6176858948.csv', header=None,index_col=False)
# df = read_csv('/home/nguyen/spark-lab/spark-2.1.1-bin-hadoop2.7//google_cluster_analysis/results/resource_usage_twoMinutes_6176858948.csv', header=None,index_col=False)

df1 = df.replace(np.nan, 0, regex=True)
df1.to_csv('thangbk2209/google_cluster_analysis/results/fiveMinutes_6176858948_notNan.csv', index=False, header=None)
# print df1