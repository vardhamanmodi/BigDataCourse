from pyspark import SparkContext
import json

sc = SparkContext()
pagecounts = sc.textFile("/Data/TweetData/championsleague_1.json")
nonemptyLines = pagecounts.filter(lambda line: line != "")
verbRDD = nonemptyLines.map(lambda line: json.loads(line)['verb'])
wordcount = verbRDD.map(lambda x: (x, 1)).reduceByKey(lambda x,y:x+y).map(lambda x:(x[1],x[0])).sortByKey(False)
wordcount.collect()