import pandas as pd
import numpy as np
from pandas import read_csv
df = read_csv('CPU-CMU.csv', header=None,index_col=False)
a = df.values
print a[0]

vl=np.append(a[0],[1,2])
print vl
m = pd.DataFrame(a)
m.to_csv('hehe.csv')
b=[]
for i in range(len(a)):
	b.append(i)
	# print b
# Arr = [10,12,13]
# ArrDf=pd.DataFrame(Arr)
new_column = pd.DataFrame({'i':b})
df = df.merge(new_column, left_index = True, right_index = True)
df.to_csv('filename.csv')