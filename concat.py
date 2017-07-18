fout=open("out2.csv","a")
# first file:
for line in open("filename.csv"):
    fout.write(line)
# now the rest:    
f = open("filename1.csv")
for line in f:
    fout.write(line)
f.close() # not really needed
fout.close()
