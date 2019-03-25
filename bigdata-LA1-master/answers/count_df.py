from pyspark.sql import SparkSession
from pyspark.sql import Row
import sys
import csv
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)

spark = SparkSession \
    .builder \
    .appName("basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
#df_wh = spark.read.csv("/home/s/s_basant/Desktop/bigdata/bigdata-LA1-master/data/frenepublicinjection2016.csv" ,header=True)
data_file = sys.argv[1] 
df_wh = spark.read.csv(data_file,header = True)
print(df_wh.count())
