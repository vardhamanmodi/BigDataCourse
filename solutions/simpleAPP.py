"""SimpleApp.py"""
from pyspark import SparkContext

datafile = "/Data/TweetData/championsleague_1.json"  # Should be some file on your system

sc = SparkContext()
tweetData = sc.textFile(datafile).cache()

numBarcelonas = tweetData.filter(lambda line: 'Barcelona' in line).count()
numBayerns = tweetData.filter(lambda line: 'Bayern' in line).count()

print "Lines with Barcelona: %i, lines with Bayern: %i" % (numBarcelonas, numBayerns)