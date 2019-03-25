
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from collections import Counter
import sys
import csv
import collections
from pyspark.sql.functions import lit
from pyspark.sql.functions import col, size
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.functions import desc

if len(sys.argv)!=5:
     print('check number of  parameters!')
     exit(1)
sc = SparkContext('local')
spark = SparkSession(sc)
def splitvalue(l):
        a = l.split(',')[0:1][0]
        b = l.split(',')[1:]
        return ( a , b )

data_file = sys.argv[1]
#data_file = '/home/s/s_basant/Desktop/bigdata/lab3/bigdata-LA3-master/data/plants.data'
# steps to create dataframe from the input
rdd = sc.textFile(data_file).map(lambda l :splitvalue(l) )
count = rdd.count()
df = spark.createDataFrame(rdd)
df = df.withColumnRenamed("_1","plant").withColumnRenamed("_2", "items")
df.createOrReplaceTempView("df")
# to create ID column
df = spark.sql("select row_number() over (order by plant) as num, * from df")
df=df.withColumn('id', df['num'] -1 ).drop("num")
# to arrange column in a order
df = df.select ( 'id','plant', 'items')
# invoke FPgrowth API
fpGrowth = FPGrowth(itemsCol="items", minSupport=float(sys.argv[3]) , minConfidence=float(sys.argv[4]))
#fpGrowth = FPGrowth(itemsCol="items", minSupport=.1 , minConfidence=.1)
model = fpGrowth.fit(df)
df_freq = model.freqItemsets
df_asso = model.associationRules
df= df_asso.join(df_freq, df_freq.items == df_asso.consequent)
df = df.withColumn('items_size', size(col('antecedent')) )
df = df.withColumn('interest', df['confidence']- df['freq']/count)
df = df.orderBy(desc("items_size"), desc("interest")).drop("items_size")
df.show(int(sys.argv[2]))
