
#import numpy import long
from pyspark import Row
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when
from pyspark.sql.functions import lit
from copy import deepcopy
if len(sys.argv)<3:
    print('check number of  parameters!')
    exit(1)
spark = SparkSession.builder.appName("Python Spark basic example").config("spark.some.config.option", "some-value").getOrCreate()
lines = spark.read.text("data/sample_movielens_ratings.txt").rdd
parts = lines.map(lambda row: row.value.split("::"))
ratingsRDD = parts.map(lambda p: Row(userId=int(p[0]), movieId=int(p[1]),rating=float(p[2]), timestamp=int(p[3])))
ratings = spark.createDataFrame(ratingsRDD)
(training, test) = ratings.randomSplit([0.8, 0.2], seed = int(sys.argv[1]))

# from training set calculte mean group by userId
user_mean_df = training.groupby('userId').agg({'rating':'mean'}).withColumnRenamed('avg(rating)', 'user-mean')
# from training set calculte mean group by movieId
item_mean_df = training.groupby('movieId').agg({'rating': 'mean'}).withColumnRenamed('avg(rating)', 'item-mean')
# from training set calculte global mean
global_mean = training.agg({'rating': 'mean'}).collect()[0][0]

# join the user-mean & item-mean & global_mean in training table
training = training.join(user_mean_df,['userId'])
training = training.join(item_mean_df,['movieId'])
training = training.withColumn('global_mean', lit(global_mean))
#from item-mean , user-mean and global mean calculate user-item-interaction :  rating - (user_mean+item_mean-global_mean)
training = training.withColumn('user-item-interaction',  training['rating'] -( training['user-mean']+ training['item-mean'] - training['global_mean']) )
#display Value
means_and_interaction = training.select('userId', 'movieId', 'rating', 'user-mean', 'item-mean', 'user-item-interaction').orderBy('userId','movieId')
means_and_interaction.show(int(sys.argv[2]))
