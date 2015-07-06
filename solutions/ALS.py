from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

# Load and parse the data
sc = SparkContext()
data = sc.textFile("/Data/ALS/*")
ratings = data.map(lambda l: l.split(',')).map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))

# Build the recommendation model using Alternating Least Squares
rank = 10
numIterations = 20
model = ALS.train(ratings, rank, numIterations)

# Evaluate the model on training data
testdata = ratings.map(lambda p: (p[0], p[1]))
#print(testdata)
predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
predictions.saveAsTextFile('vardhaman.txt')