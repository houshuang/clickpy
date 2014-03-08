# TODO Fix indexing, differs between video and non video wrt timestamps. Make sure all columns are available.
# run across all students, time it, on the MRI with tmux

import pandas as pd
import numpy as np
from pandas import DataFrame, Series
import itertools
import sys
import os
import pickle
import pprint
from uuid import uuid4
pp = pprint.PrettyPrinter(indent=2).pprint

from action_converter import ActionConverter


def dump(arr, tmpdir):
	"""Dumps lines to a text file in tmp dir"""

	idstr = str(uuid4())

	with open(os.path.join(tmpdir, idstr), "wt") as dumpf:
		dumpf.write(''.join(arr))

	print("*** Dumping to %s" % idstr)

#**************************************************
argv = sys.argv
if len(argv) < 5:
	print("Usage: store.h5 range-start range-jump working-dir")
	exit()

store = pd.HDFStore(argv[1])
converter = ActionConverter(store)
store_length = len(store['username'])
range_start = int(argv[2])
range_jump = int(argv[3])
working_dir = argv[4]
print(store_length)
event_arr = []
if len(argv) > 5:
        time_cutoff = int(argv[5])
else:
        time_cutoff = None

i = 0
cnt = 0
c = range_start

while c < store_length + 1:
	i += 1
	cnt += 1

	print("%d (%d): Processing user %d: " % (i, cnt, c))
	events = converter.convert(c, max_time = time_cutoff)
	if not events is None:
		event_arr.append(events)
	else:
		print("None")
	if i > 10:
		i = 0
		dump(event_arr, working_dir)
		event_arr = []
	c += range_jump

if i > 0:
	dump(event_arr, working_dir)
