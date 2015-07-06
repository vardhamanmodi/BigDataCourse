"""Finding Kmers"""
from pyspark import SparkContext
import json

# Function definition is here
def find_kmers(string, k):
   kmers = []
   for i in range(0, 102-k):
      kmers.append(string[i:i+k])
   return ((word, 1) for word in kmers)


sc = SparkContext()
#load genomics dataset
records= sc.textFile("/Data/GenomicsData/*")

#kepp only lines with length 101, filter rest
sequence = records.filter(lambda line : len(line)==101)

kmers = sequence.flatMap(lambda line : find_kmers(line,8)).map(lambda x: (x, 1)).reduceByKey(lambda x,y:x+y).map(lambda x:(x[1],x[0])).sortByKey(False)
kmers.saveAsTextFile('kmers.txt')