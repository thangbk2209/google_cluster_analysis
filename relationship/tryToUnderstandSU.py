import matplotlib as mpl
mpl.use('Agg')
import math
import matplotlib.pyplot as plt
from pandas import read_csv
from pandas.plotting import autocorrelation_plot
from pandas.plotting import lag_plot
from matplotlib.backends.backend_pdf import PdfPages
from sklearn.linear_model import LinearRegression as LR
import numpy as np
import pandas as pd
from scipy.stats import entropy

from collections import Counter
# Counter(array1.most_common(1))
# print math.log(2,2)

# a =[1,2,3,4,5,1,2]
# print Counter(a.most_common(1))

def entro(X):
	x = [] # luu lai danh sach cac gia tri X[i] da tinh
	tong_so_lan = 0
	result = 0
	p=[]
	for i in range(len(X)):
		if Counter(x)[X[i]]==0:
			so_lan = Counter(X)[X[i]]
			tong_so_lan += so_lan
			x.append(X[i])
			P = 1.0*so_lan / len(X)
			p.append(P)
			result -= P * math.log(P,2)
		if tong_so_lan == len(X):
			break
	return result
def entroXY(X,Y):
	y = []
	result = 0
	pY = []
	tong_so_lan_Y = 0
	for i in range(len(Y)):
		# print Counter(y)[Y[i]]
		if Counter(y)[Y[i]]==0:
			x=[]
			so_lan_Y = Counter(Y)[Y[i]]
			tong_so_lan_Y += so_lan_Y
			y.append(Y[i])
			PY = 1.0* so_lan_Y / len(Y)
			# vi_tri = Y.index(Y[i])
			vi_tri=[]
			for k in range(len(Y)):
				if Y[k] == Y[i]: 
					vi_tri.append(k)
			for j in range(len(vi_tri)):
				x.append(X[vi_tri[j]])
			entro_thanh_phan = entro(x)
			result += PY * entro_thanh_phan
		if tong_so_lan_Y == len(Y):
			break
	return result
def infomation_gain(X,Y):
	return entro(X) - entroXY(X,Y)
def symmetrical_uncertainly(X,Y):
	return 2.0*infomation_gain(X,Y)/(entro(X)+entro(Y))

X = [1,2,3,1,2,4,1]
Y = [0.5,0.6,0.5,0.4,0.5,0.4,0.5]
Z = [0.2,0.3,0.3,0.5,0.6,0.4,0.7]

eX = entro(X)
print 'entroX'
print eX
eY = entro(Y)
print 'entroY'
print eY
eZ = entro(Z)
print 'entroZ'
print eZ

eXY = entroXY(X,Y)
print 'entroXY'
print eXY
eYX = entroXY(Y,X)
print 'entroYX'
print eYX
eXZ = entroXY(X,Z)
print 'entroXZ'
print eXZ
eYZ = entroXY(Y,Z)
print 'entroYZ'
print eYZ
if eXY != eYX:
	eZX = entroXY(Z,X)
	print 'entropyZX = '
	print eZX
	eZY = entroXY(Z,Y)
	print 'entropyZY = '
	print eZY

IGXY = eX - eXY
IGYX = eY - eYX
print "IGXY: "
print IGXY
print "IGYX: "
print IGYX
IGXZ = eX - eXZ
print "IGXZ: "
print IGXZ
IGYZ = eY - eYZ
print "IGYZ: "
print IGYZ
if IGYX != IGXY:
	IGZX = eZ - entroXY(Z,X)
	print "IGZX: "
	print IGZX
	IGZY = eZ - entroXY(Z,Y)
	print "IGZY: "
	print IGZY

print 'XY'
print symmetrical_uncertainly(X,Y)
print 'YZ'
print symmetrical_uncertainly(Y,Z)
print 'ZX'
print symmetrical_uncertainly(Z,X)