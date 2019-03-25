from pyspark import SparkContext
from pyspark import SparkConf
from random import sample, seed
from pyspark import SparkContext
from collections import Counter

import sys

from all_states import all_states
if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("invlid no of arguments")
        exit(-1)     
sc = SparkContext('local')
data_file = sys.argv[1]

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


def find_Centroid(class_List):
	New_centroid_coordinate =[]
	num= 0
	for s in class_List:
		c = s.split(" ")
		rdd_city =   rdd3.filter(lambda l: findPlantCity(l,c))
		city_coordinate = rdd_city.mapValues(list).collect()
		plant = []
		for items in  city_coordinate:
			for value in items[1]:
				for k , v in value.items():
					plant.append(k)
		Plant_count = Counter(plant)
		no_centroid = len(c)
		New_Centroid = []
		for k, v in Plant_count.items():
			n= v
			d = no_centroid
			f= n/d
			a = {k:f}
			New_Centroid.append(a)
		Center_Name = "C" +repr(num)
		num= num +1
		New_centroid_coordinate.append((Center_Name,New_Centroid ))
	return(New_centroid_coordinate)


def Find_Closest_class(New_centroid_coordinate):	
	new_closest_class = []	
	for state in state_coordinate:
		min_distance = 999999
		center = ""
		state_point = state[1:][0]
		state_name = state[0]
		center = "null"
		for centroid in New_centroid_coordinate:
			state_center = centroid[0]
			center_point = centroid[1]
			length = distance(state_point, center_point)
			if(length < min_distance):
				min_distance = length
				center= state_center
		new_closest_class.append({center : state_name})
	return(new_closest_class)

def format_class(new_closest_class):
	new_class_group = {}
	for values in new_closest_class:
		for k,v in values.items():
			new_class_group.setdefault(k, [])
			new_class_group[k].append(v)
	sorted_class_List = []
	for k , v in new_class_group.items():
		value = sorted(v)
		sorted_class_List.append((k,value))
	updated_cluster_list = []
	for item in sorted_class_List:
		strv = " "
		a = item[1:][0]
		for value in a:
			strv = strv +value+" "
		updated_cluster_list.append(strv.lstrip())
	return(updated_cluster_list)

def kmeanIteration(old_cluster_list):
	New_centroid_coordinate = find_Centroid(old_cluster_list)
	new_closest_class = Find_Closest_class(New_centroid_coordinate)
	updated_cluster_list = format_class(new_closest_class)
	updated_cluster_list.sort()
	return updated_cluster_list


max_it=100
old_cluster_list = class_List
new_cluster_list = []
for i in range(0, max_it):
	new_cluster_list =  kmeanIteration(old_cluster_list)
	if sorted(new_cluster_list) == sorted(old_cluster_list):
		break
	old_cluster_list = new_cluster_list

#G_count = 0
#for items in old_cluster_list:
#    print("* Class "+ repr(G_count))
#    print(items)
#    G_count= G_count+1
count = 0
if len(sys.argv) == 5:
	with open(sys.argv[4], 'w') as text_file:
		for items in old_cluster_list:
			text_file.write("* Class "+ repr(count))
			text_file.write("\n")
			text_file.write(items)
			text_file.write("\n")
			count= count+1




