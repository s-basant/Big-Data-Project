from pyspark import SparkContext
from pyspark import SparkConf
import csv
import sys
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: parks_rdd.py <data_file>", file=sys.stderr)
        exit(-1)

sc = SparkContext('local')
data_file = "/home/s/s_basant/Desktop/bigdata/bigdata-LA1-master/data/frenepublicinjection2016.csv"
raw_data = sc.textFile(data_file)
raw_data_wh = raw_data.zipWithIndex().filter(lambda kv: kv[1] > 0).keys()

def my_csv_reader(line):
	for col in csv.reader([line], delimiter = ',', quotechar = '"'):
		return col[6]

raw_data_wh_filter = raw_data_wh.filter(lambda line: my_csv_reader(line)!="" )
print(raw_data_wh_filter.count())











