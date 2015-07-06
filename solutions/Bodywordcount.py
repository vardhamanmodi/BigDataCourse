"""Word count on tweet body"""
from pyspark import SparkContext
import json

sc = SparkContext()
pagecounts = sc.textFile("/Data/TweetData/*")
nonemptyLines = pagecounts.filter(lambda line: line != "")
verbRDD = nonemptyLines.map(lambda line: json.loads(line)['body'])

wordcount = verbRDD.map( lambda x: x.replace(',',' ').replace('.',' ').replace('-',' ').lower()) \
        .flatMap(lambda x: x.split()) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda x,y:x+y) \
        .map(lambda x:(x[1],x[0])) \
        .sortByKey(False)

wordcount.saveAsTextFile('wordcount.txt')