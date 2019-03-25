import sys
import csv

if len(sys.argv)<2:
    print('Invalid number of input parameters!')
    exit(1)
mySet = set()
count = 0
with open(sys.argv[1]) as inputFile:
    reader = csv.reader(inputFile, delimiter=',')
    next(reader)
    for row in reader:
        if row[6]:
            mySet.add(row[6])
            count += 1
sorted_set = sorted(mySet)
for element in sorted_set:
    print(element)

