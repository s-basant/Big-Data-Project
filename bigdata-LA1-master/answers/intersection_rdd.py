from pyspark import SparkContext
from pyspark import SparkConf
import sys
import csv
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: intersection_rdd.py <data_file_1> <data_file_2>", file=sys.stderr)
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
distinct_data = raw_data_wh_map.distinct()
data_file_2 = sys.argv[2]
raw_data_2 = sc.textFile(data_file_2)
raw_data_wh_2 = raw_data_2.zipWithIndex().filter(lambda kv: kv[1] > 0).keys()
raw_data_wh_filter_2 = raw_data_wh_2.filter(lambda  line : my_csv_reader(line)!="")
raw_data_wh_map_2= raw_data_wh_filter_2.map(lambda line : my_csv_reader(line) )
distinct_data_2 = raw_data_wh_map_2.distinct()
common_Park = sorted ( distinct_data.intersection(distinct_data_2).collect())
for park in common_Park:
    print(park)
