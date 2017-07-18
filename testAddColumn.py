import pandas as pd
from pandas import read_csv
df = read_csv('CPU-CMU.csv', header=None,index_col=False)
a = df.values
m = pd.DataFrame(a)
m.to_csv('hehe.csv')
b=[]
for i in range(len(a)):
	b.append(i)
# Arr = [10,12,13]
# ArrDf=pd.DataFrame(Arr)
new_column = pd.DataFrame({'i':b})
df = df.merge(new_column, left_index = True, right_index = True)
df.to_csv('filename.csv')