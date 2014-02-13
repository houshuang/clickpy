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

def dump(arr, tmpdir):
    """Pickle's the array, and stores it in tmpdir. Uses rename to
    ensure writes are atomic
    """

    idstr = str(uuid4())

    with open(os.path.join(tmpdir, "sub", idstr), "wb") as dumpf:
        pickle.dump(arr, dumpf)
    os.rename(os.path.join(tmpdir, "sub", idstr),
        os.path.join(tmpdir, idstr))

    print("*** Dumping to %s" % idstr)

def join_value(frame, store, values):
	for val in values:
		val_series = store[val].to_frame().reset_index().rename(columns={'index':val + '_val', 0: val})
		val_series = val_series.set_index(val)
		frame = frame.set_index(val)
		frame = frame.join(val_series, how='outer')
		frame = frame.reset_index()
	return(frame)

def convert_user_events(user):
	u = store.select('db', pd.Term('username = %s' % user))

	u = u.reset_index()
	u.timestamp = pd.to_datetime(u.timestamp, unit='s')
	u = u.sort(columns = 'timestamp')
	u['duration'] = u.timestamp.diff()
	u.duration = u.duration.shift(-1)
	tuples = [tuple(x) for x in u[['timestamp', 'action', 'lecture_id']].to_records(index=False)]
	video_sections = [list(g) for k,g in itertools.groupby(tuples, lambda x: (x[1], x[2]))]

	u = join_value(u, store, ['type'])
	video_events = []
	u = u.set_index('timestamp')
	for segment in video_sections:
		rows = []
		for event in segment:
			rows.append(event[0])

		r = u.loc[rows]

		if r.head(1).action.values[0] != lecture_action:
			reduce_dict = r.reset_index()[['duration', 'action']].to_dict(outtype='records')[0]
		else:
			reduce_dict = r.type_val.value_counts().to_dict()
			duration = r.duration.sum()
			reduce_dict.update({"duration": duration,
								"lecture_id": r.head(1).lecture_id.values[0], "action": lecture_action})
		timestamp = pd.to_datetime(r.head(1).index.item())
		reduce_dict.update({"timestamp": timestamp})

		video_events.append(reduce_dict)

	return(DataFrame(video_events, columns=['action', 'duration', 'lecture_id', 'pause', 'play', 'ratechange', 'seeked', 'stalled', 'timestamp']))

#**************************************************
argv = sys.argv
if len(argv) < 5:
	print("Usage: store.h5 range-start range-end working-dir")
	exit()

store = pd.HDFStore(argv[1])
lecture_action = store['action']['lecture/view']

ids = range(int(argv[2]), int(argv[3]))
working_dir = argv[4]

event_arr = []

i = 0

for username in ids:
	i += 1
	events = convert_user_events(username)
	event_arr.append(events)
	if i > 10:
		i = 0
		dump(pd.concat(event_arr), working_dir)

if i > 0:
	dump(pd.concat(event_arr), working_dir)
