import sys
import csv
from collections import Counter

if len(sys.argv)<2:
    print('Invalid number of input parameters!')
    exit(1)
Parks = dict()
with open(sys.argv[1]) as csvFile:
    reader = csv.reader(csvFile, delimiter=',')
    next(reader)
    for row in reader:
        if row[6]:
            if not row[6] in Parks:
                Parks[row[6]] = 1
            else:
                Parks[row[6]] += 1
for key, value in Counter(Parks).most_common(10):
    print("%s: %s" % (key, value))


