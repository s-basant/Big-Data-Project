import sys
import csv
from collections import OrderedDict

if len(sys.argv) < 2:
    print('Invalid number of input parameters!')
    exit(1)
park_List = dict()
count = 0
with open(sys.argv[1]) as inputFile:
    reader = csv.reader(inputFile, delimiter=',')
    next(reader)
    for row in reader:
        if row[6]:
            if not row[6] in park_List:
                park_List[row[6]] = 1
            else:
                park_List[row[6]] += 1
for key in OrderedDict(sorted(park_List.items())):
    print("%s: %s" % (key, park_List[key]))

