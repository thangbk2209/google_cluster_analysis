fout=open("thangbk2209/google_cluster_analysis/results/minMaxTimePart_resoure_6176858948.csv","a")
for num in range(0,500):
    f = open("thangbk2209/minMaxTimePart_resoure_6176858948/part-00"+str(num).zfill(3)+"-of-00500.csv")
    for line in f:
         fout.write(line)
    f.close() # not really needed
fout.close()
