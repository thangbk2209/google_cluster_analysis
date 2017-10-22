from pandas import read_csv
import numpy as np
import matplotlib.pyplot as plt
colnames = ['lcavol','lweight','age','lbph','svi','lcp','gleason','pgg45','lpsa']
df = read_csv('/home/nguyen/ML2017/Ridge_Regression/data/1-prostate-training-data.csv', header=None, index_col=False, names= colnames, engine='python')
print 1
data = df['lcavol'].values
print data
