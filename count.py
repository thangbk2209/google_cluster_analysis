import pandas as pd


startTimeDf = pd.read_csv('/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/TopJobId/TopJobId-startTime.csv', header=None)
# split dataset
startTimeArr = startTimeDf.values
print len(startTimeArr)