import sys
import csv

if len(sys.argv) < 2:
    print('Invalid number of input parameters!')
    exit(1)
with open(sys.argv[1]) as inputFile:
    count = 0
    reader = csv.reader(inputFile, delimiter=',')
    next(reader)
    for row in reader:
        count += 1
print(count)

