
from pyspark import SparkContext
import sys
if len(sys.argv)!=5:
     print('check number of  parameters!')
     exit(1)
sc = SparkContext('local')

#data_file = '/home/s/s_basant/Desktop/bigdata-LA3-master/data/plants.data'
data_file = sys.argv[1]
state = sys.argv[3]
plant = sys.argv[2]
def createdict(l):
		c =[]
		a = l.split(',')[0:1][0]
		b = l.split(',')[1:]
		for x in b:
			c.append((x,{a:1}))
		return c
		
def findPlantCity(item):
	for tup in item[0:1]:
		if(tup==state):
			for tup in item[1:]:
				for k , v in tup.items():
					if(k==plant):
						return True

rdd1 = sc.textFile(data_file).flatMap(lambda l :createdict(l)).filter(lambda l: findPlantCity(l) )


with open(sys.argv[4], 'w') as text_file:
	if(rdd1.count() > 0):
		text_file.write('1\n')
	else:
		text_file.write('0\n')
		

