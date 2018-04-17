import pandas as pd
import numpy as np
import os
from pandas import read_csv

file_names = ['resource_usage_3Minutes_6176858948.csv']
file_results = ['3Minutes_6176858948_notNan.csv']
for i in range(1):
    file_name = file_names[i]
    file_result = file_results[i]
    df = read_csv('thangbk2209/google_cluster_analysis/results/%s'%(file_name), header=None,index_col=False)
    # df = read_csv('/home/nguyen/spark-lab/spark-2.1.1-bin-hadoop2.7//google_cluster_analysis/results/resource_usage_twoMinutes_6176858948.csv', header=None,index_col=False)

    df1 = df.replace(np.nan, 0, regex=True)
    df1.to_csv('thangbk2209/google_cluster_analysis/results/%s'%(file_result), index=False, header=None)
    # print df1