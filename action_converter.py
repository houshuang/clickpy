import pandas as pd
import numpy as np
from pandas import DataFrame, Series
import itertools

def join_value(frame, store, values):
	for val in values:
		val_series = store[val].to_frame().reset_index().\
			rename(columns={'index':val + '_val', 0: val})
		val_series = val_series.set_index(val)
		frame = frame.set_index(val)
		frame = frame.join(val_series, how='outer')
		frame = frame.reset_index()
	return(frame)

class ActionConverter(object):
	def __init__(self, store):
		self.store = store
		self.store_length = store['db'].username.max()
		self.lecture_action = store['action']['lecture/view']

	def convert(self, user):
		store = self.store
		u = store.select('db', pd.Term('username = %s' % user))

		if len(u) == 0:
			return(None)
		print(len(u))
		u = u.reset_index()
		u.timestamp = pd.to_datetime(u.timestamp, unit='s')
		u = u.sort(columns = 'timestamp')
		u['duration'] = u.timestamp.diff()
		u.duration = u.duration.shift(-1)
		tuples = [tuple(x) for x in u[['timestamp', 'action', 'lecture_id']].
				  to_records(index=False)]
		video_sections = [list(g) for k,g in itertools.
						  groupby(tuples, lambda x: (x[1], x[2]))]

		u = join_value(u, store, ['type'])
		video_events = []
		u = u.set_index('timestamp')
		for segment in video_sections:
			rows = []
			for event in segment:
				rows.append(event[0])

			r = u.loc[rows]

			if r.head(1).action.values[0] != self.lecture_action:
				reduce_dict = r.reset_index()[['duration', 'action']].\
					to_dict(outtype='records')[0]
			else:
				reduce_dict = r.type_val.value_counts().to_dict()
				duration = r.duration.sum()
				reduce_dict.update({"duration": duration,
									"lecture_id": r.head(1).lecture_id.values[0],
									"action": self.lecture_action})
			timestamp = pd.to_datetime(r.head(1).index.item())
			reduce_dict.update({"timestamp": timestamp, "username": user})

			video_events.append(reduce_dict)

		return(DataFrame(video_events, columns=['action', 'duration', 'lecture_id',
			'pause', 'play', 'ratechange', 'seeked', 'stalled', 'timestamp', 'username']))
