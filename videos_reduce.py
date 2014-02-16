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

import action_converter


def dump(arr, tmpdir):
    """Pickles the array, and stores it in tmpdir. Uses rename to
    ensure writes are atomic
    """

    idstr = str(uuid4())

    with open(os.path.join(tmpdir, "sub", idstr), "wb") as dumpf:
        pickle.dump(arr, dumpf)
    os.rename(os.path.join(tmpdir, "sub", idstr),
        os.path.join(tmpdir, idstr))

    print("*** Dumping to %s" % idstr)

#**************************************************
argv = sys.argv
if len(argv) < 5:
	print("Usage: store.h5 range-start range-jump working-dir")
	exit()

converter = action_converter(store)

range_start = int(argv[2])
range_jump = int(argv[3])
working_dir = argv[4]

event_arr = []

i = 0
cnt = 0
c = range_start

while c < store_length + 1:
	i += 1
	cnt += 1

	print("%d (%d): Processing user %d: " % (i, cnt, c))
	events = converter.convert(c)
	if not events is None:
		event_arr.append(events)
	else:
		print("None")
	if i > 10:
		i = 0
		dump(pd.concat(event_arr), working_dir)
		event_arr = []
	c += range_jump

if i > 0:
	dump(pd.concat(event_arr), working_dir)
