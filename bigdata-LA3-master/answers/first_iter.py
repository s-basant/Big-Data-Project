from pyspark import SparkContext
from pyspark import SparkConf
from random import sample, seed
from pyspark import SparkContext
import sys

from all_states import all_states
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: intersection_rdd.py <data_file_1> <data_file_2>")
        exit(-1)     
sc = SparkContext('local')
data_file = sys.argv[1]

#s_centroid = sample(all_states, int( sys.argv[2]))
seed(int( sys.argv[3]))
s_centroid = sample(all_states, int( sys.argv[2]))

def createdict(l):
    c = []
    a = l.split(',')[0:1][0]
    b = l.split(',')[1:]
    for x in b:
        c.append((x, {a: 1}))
    return c

def findPlantCity(list, state):
    for tup in list[0:1]:
        if (tup in state):
            return True

def distance(plant1, plant2):
	dict1 = {}
	dict2 = {}
	for items in plant1:
		dict1.update(items)
	for items in plant2:
		dict2.update(items)
	all_items =  set(dict1).union(set(dict2))
	distance = 0
	distance = 0
	for key in all_items:
		value1= 0
		value2= 0
		if key in dict1:
			value1 = dict1[key]
		if key in dict2:
			value2 =  dict2[key]
		d =abs( value1 - value2)
		d=d*d
		distance = distance + d
	return(distance)

inputstate = sc.textFile("./data/stateabbr.txt")
state= inputstate.zipWithIndex().filter(lambda kv: kv[1] > 2).keys().collect()

state_list = []
for elem in state:
    if(elem!='Canada:'):
        state_list.append((elem.split(" "))[0])

state_list =list( filter(None, state_list))
rdd1 = sc.textFile(data_file).flatMap(lambda l: createdict(l))


rdd3 = rdd1.groupByKey()
rdd_centroid = rdd3.filter(lambda l: findPlantCity(l,s_centroid))
rdd_state =   rdd3.filter(lambda l: findPlantCity(l,state_list))
centroid_coordinate = rdd_centroid.mapValues(list).collect()
state_coordinate = rdd_state.mapValues(list).collect()


closest_class = []
for state in state_coordinate:
    min_distance = 999999
    center = ""
    state_point = state[1:][0]
    state_name = state[0]
    center = "null"
    for centroid in centroid_coordinate:
        state_center = centroid[0]
        center_point = centroid[1:][0]
        length = distance(state_point, center_point)
        if(length < min_distance):
            min_distance = length
            center= state_center
    closest_class.append({center : state_name})



class_group = {}
for values in closest_class:
   for k,v in values.items():
       class_group.setdefault(k, [])
       class_group[k].append(v)
       sorted(class_group[k])


class_List = []
for k , v in class_group.items():
    str = " "
    value = sorted(v)
    for item in value:
        str = str +item+" "
    class_List.append(str.lstrip())
class_List.sort()
count = 0
for items in class_List:
    print("* Class "+ repr(count))
    print(items)
    count= count+1




