
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
data_file = sys.argv[1] 
df_wh = spark.read.csv(data_file, header=True)
df_wh.registerTempTable("montreal_forest")
uniq_parks = spark.sql("select distinct nom_parc from montreal_forest where nom_parc is not null order by nom_parc ASC " )

for data in uniq_parks.rdd.flatMap(lambda x: x).collect():
	print(data)


