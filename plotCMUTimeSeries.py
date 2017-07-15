import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from pandas import Series
from pandas.plotting import autocorrelation_plot
from pandas.plotting import lag_plot
from matplotlib.backends.backend_pdf import PdfPages

fig = plt.figure(figsize=(10,20))
series = Series.from_csv( 'CMUTimeSeries.csv' , header=0)
ax0 = plt.subplot2grid((4,1),(0,0))
ax0.set(title="job Id = 3537558367", xlabel="Seconds", ylabel="canonical memory usage")
series.plot(color = ['b'],ax=ax0)

ax1 = plt.subplot2grid((4,1),(1,0))
series.hist(ax=ax1)

ax2 = plt.subplot2grid((4,1),(2,0))
series.plot(kind='kde',ax=ax2)

ax3 = plt.subplot2grid((4,1),(3,0))
autocorrelation_plot(series,ax=ax3)
plt.show()
pp = PdfPages('CMU.pdf')
pp.savefig()
pp.close()
