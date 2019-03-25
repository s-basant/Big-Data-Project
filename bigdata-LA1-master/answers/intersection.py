import sys
import csv
if len(sys.argv) < 3:
    print('Invalid number of input parameters!')
    exit(1)
InptutSet = set()
InptutSet2 = set()
count1 = 0
count2 = 0
with open(sys.argv[1]) as inputFile1, open(sys.argv[2], encoding="utf8") as inputFile2:
    reader1 = csv.reader(inputFile1, delimiter=',')
    reader2 = csv.reader(inputFile2, delimiter=',')
    next(reader1)
    next(reader2)
    for row in reader1:
        if row[6]:
            InptutSet.add(row[6])
            count1 += 1
    for row in reader2:
        if row[6]:
            InptutSet2.add(row[6])
            count2 += 1
u = set.intersection(InptutSet, InptutSet2)
for element in sorted(u):
    print(element)

