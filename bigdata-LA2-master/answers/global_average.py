from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
import sys
spark = SparkSession \
    .builder \
    .appName("basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

lines = spark.read.text("/home/s/s_basant/Desktop/bigdata/bigdata-LA2-master/data/sample_movielens_ratings.txt").rdd
parts = lines.map(lambda row: row.value.split("::"))
ratingsRDD = parts.map(lambda p: Row(userId=int(p[0]), movieId=int(p[1]), rating=float(p[2]), timestamp=int(p[3])))
ratings = spark.createDataFrame(ratingsRDD)
(training, test) = ratings.randomSplit([0.8, 0.2], seed = int(sys.argv[1]))
als= ALS(maxIter=5, regParam=0.01,rank= 70,  userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop").setSeed(int(sys.argv[1]))
model = als.fit(training)
predictions = model.transform(test).groupBy().avg('rating').collect()


