
from pyspark import SparkContext
from pyspark import SparkConf
from collections import OrderedDict
from collections import Counter
import sys
import csv

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage:frequent_parks_count_rdd <file>", file=sys.stderr)
        exit(-1)

sc = SparkContext('local')
data_file = sys.argv[1]
raw_data = sc.textFile(data_file)
raw_data_wh = raw_data.zipWithIndex().filter(lambda kv: kv[1] > 0).keys()
def my_csv_reader(line):
        for col in csv.reader([line], delimiter = ',', quotechar = '"'):
                return col[6]
raw_data_wh_filter = raw_data_wh.filter(lambda  line : my_csv_reader(line)!="")
raw_data_wh_map= raw_data_wh_filter.map(lambda line : my_csv_reader(line) )

park_count = raw_data_wh_map.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

park_count_dict = park_count.collectAsMap()
for key, value  in Counter(park_count_dict).most_common(10):
	print("%s: %s" % (key, value))
