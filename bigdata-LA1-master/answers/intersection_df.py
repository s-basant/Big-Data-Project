
from pyspark.sql import SparkSession
from pyspark.sql import Row
from collections import Counter
import collections
import sys
import csv
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)

spark = SparkSession \
    .builder \
    .appName("basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
data_file = sys.argv[1] 
data_file_2 = sys.argv[2] 
df_wh = spark.read.csv(data_file,header=True)
df_wh_nn= df_wh.filter(df_wh['nom_parc'] != "") 

df_wh_2 = spark.read.csv(data_file_2,header=True)
df_wh_nn_2 =  df_wh_2.filter(df_wh_2['nom_parc'] != "") 

#common_Park_df = (df_wh_nn.select('nom_parc')).intersect(df_wh_nn_2.select('nom_parc'))

#common_Park_df = (df_wh.select('nom_parc')).intersect(df_wh_2.select('nom_parc'))

#common_Park _df= sorted ( (df_wh_nn.select('nom_parc')).intersect(df_wh_nn_2.select('nom_parc')).collect())


common_Park_df = sorted ( (df_wh_nn.select('nom_parc')).intersect(df_wh_nn_2.select('nom_parc')).collect())


common_Park_List = []
for items in common_Park_df:
	 common_Park_List.append(items[0])


for park in common_Park_List:
    print(park)
