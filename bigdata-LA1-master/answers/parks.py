import sys
import csv

if len(sys.argv) < 2:
    print('Invalid number of Input argument!')
    exit(1)
count = 0
with open(sys.argv[1]) as inputFile:
    reader = csv.reader(inputFile, delimiter=',')
    next(reader)
    for row in reader:
        if row[6]:
            count += 1
print(count)

