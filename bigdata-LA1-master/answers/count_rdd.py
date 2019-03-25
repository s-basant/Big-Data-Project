from pyspark import SparkContext
from pyspark import SparkConf
import sys
sc = SparkContext('local')
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: count_rdd <file>", file=sys.stderr)
        exit(-1)
data_file =sys.argv[1]
# "/home/s/s_basant/Desktop/bigdata/bigdata-LA1-master/data/frenepublicinjection2016.csv"
raw_data = sc.textFile(data_file)
raw_data_wh = raw_data.zipWithIndex().filter(lambda kv: kv[1] > 0).keys()
print(raw_data_wh.count())
