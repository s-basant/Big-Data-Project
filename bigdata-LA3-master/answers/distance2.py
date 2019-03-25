from pyspark import SparkContext
import sys
if len(sys.argv)<4:
     print('check number of  parameters!')
     exit(1)
sc = SparkContext('local')

#data_file = '/home/s/s_basant/Desktop/bigdata-LA3-master/data/plants.data'
data_file = sys.argv[1]
st1 = sys.argv[2]
st2 = sys.argv[3]

def createdict(l):
    c = []
    a = l.split(',')[0:1][0]
    b = l.split(',')[1:]
    for x in b:
        c.append((x, {a: 1}))
    return c


def findPlantCity(item):
    for tup in item[0:1]:
        if (tup == st1) or (tup == st2):
            return True

rdd1 = sc.textFile(data_file).flatMap(lambda l: createdict(l))
# State with all plants
rdd3 = rdd1.groupByKey()
rdd4 = rdd3.filter(lambda l: findPlantCity(l))

state1 = rdd4.mapValues(list).collect()[0]
state2 = rdd4.mapValues(list).collect()[1]

plant1 = state1[1:]
plant2 = state2[1:]

# 2: Difference Between Two States
dict1 = {}
dict2 = {}
for items in plant1[0]:
    dict1.update(items)


for items in plant2[0]:
    dict2.update(items)

#distinct_items = set(dict1) ^ set(dict2)
#distance = len(distinct_items)			
#distance_str = str(distance)

all_items =  set(dict1).union(set(dict2))

distance = 0
for key in all_items:
	value1= 0
	value2= 0
	if key in dict1:
		value1 = dict1[key]
	if key in dict2:
		value2 =  dict2[key]
	d =abs( value1 - value2)
	distance = distance + d

distance_str = str(distance)

if(len(sys.argv)==5):
	with open(sys.argv[4], 'w') as text_file:
        	text_file.write(distance_str+'\n')
print(distance_str)
