import pandas as pd
import numpy as np
from pandas import DataFrame, Series
import itertools
import pprint
pp = pprint.PrettyPrinter(indent=2).pprint

store = pd.HDFStore("mentalhealth_002.h5.repacked")
lecture_action = store['action']['lecture/view']

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

	video_events = []
	u = join_value(u, store, ['action', 'type', 'lecture_id'])
	u = u.set_index('timestamp')
	for segment in video_sections:
		rows = []
		for event in segment:
			rows.append(event[0])

		r = u.loc[rows]

		if r.head(1).action_val.values[0] != lecture_action:
			video_events.append(r[['duration', 'action_val']].reset_index().to_dict(outtype='records')[0])
			continue
		#print(r.ix[:,['duration','action_val', 'lecture_id_val', 'type_val']])
		reduce_dict = r.type_val.value_counts().to_dict()
		duration = r.duration.sum()
		reduce_dict.update({"duration": duration, "timestamp":pd.to_datetime(r.head(1).index.item()),
							"lecture_id": r.head(1).lecture_id.values[0], "action_val": "lecture/view"})
		video_events.append(reduce_dict)

	return(DataFrame(video_events))
