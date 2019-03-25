from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from collections import Counter
import sys
import csv
import collections
from pyspark.sql.functions import lit
if len(sys.argv)!=3:
     print('check number of  parameters!')
     exit(1)
sc = SparkContext('local')
spark = SparkSession(sc)
def splitvalue(l):
	a = l.split(',')[0:1][0]
	b = l.split(',')[1:]
	return ( a , b )
data_file = sys.argv[1]
rdd = sc.textFile(data_file).map(lambda l :splitvalue(l) )
df = spark.createDataFrame(rdd)
df = df.withColumnRenamed("_1","plant").withColumnRenamed("_2", "items")
df.createOrReplaceTempView("df")
df = spark.sql("select row_number() over (order by plant) as num, * from df")
df=df.withColumn('id', df['num'] -1 ).drop("num")
df = df.select ( 'id','plant', 'items')
df.show(int(sys.argv[2]))

