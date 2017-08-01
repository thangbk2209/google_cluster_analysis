import pandas as pd
import numpy as np
import os
from pandas import read_csv

df = read_csv('/home/nguyen/spark-lab/spark-2.1.1-bin-hadoop2.7/google_cluster_analysis/test.csv', header=None,index_col=False)
df1 = df.replace(np.nan, 0, regex=True)
df1.to_csv('hehe.csv', index=False, header=None)
# print df1