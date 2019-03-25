import sys
from random import seed
from all_states import all_states
from random import sample
if len(sys.argv)!=3:
     print('check number of  parameters!')
     exit(1)
seed(int(sys.argv[2]))
s_centroid = sample(all_states, int(sys.argv[1]))
for items in s_centroid:
    print(items)
