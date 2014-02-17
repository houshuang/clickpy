import pandas as pd
import numpy as np
from pandas import DataFrame, Series
import itertools


# iterate through events, keeping track of previously seen
# For videos, we need to tag
# 1) if they are being viewed out of the intended order,
# 2) if they have already been seen
# 3) if they are being immediately reviewed
# 4) if they are being paused, and if so are they being paused multiple times
# 5) if their playback rate is being changed, and if so is it being changed multiple times
class LectureView:
	"""Processes lecture views, keeping track of last seen, etc"""
	def __init__(self):
		self.seen = []

	def proc(self, lec):
		l = lec #row['lecture_id'][0]
		print(l,type(l))
		if self.seen == []:
			self.seen.append(l)
			return '' # no tag
		if l == self.seen[-1]:
			return 'immediate-review'
		if l in self.seen:
			self.seen.append(l)
			return 'seen-before'
		if l-1 in self.seen: # immediately following previously seen value, no tag
			self.seen.append(l)
			return 'normal'
		# end of the list, if it hasn't been caught already, should mean out-of-sequence
		self.seen.append(l)
		return 'out-of-sequence'

def dispatch(vals):
	#handlers = {'lecture/view': LectureView}
	#handlers = {k:handler() for k,v in handlers.items()} # initialize handlers
	proc = LectureView()
	return [proc.proc(v) for v in vals]
	if action in handlers.keys():
		return(handlers[action][val])
	else:
		return ''

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

		# checking if there are any events
		if len(u) == 0:
			return(None)
		print(len(u))

		# cleaning up and organizing in groups to parse videos
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

			# if not a video event, just write it in
			if r.head(1).action.values[0] != self.lecture_action:
				reduce_dict = r.reset_index()[['duration', 'action']].\
					to_dict(outtype='records')[0]

			# if a video event, reduce down to one line
			else:
				reduce_dict = r.type_val.value_counts().to_dict()
				duration = r.duration.sum()
				reduce_dict.update({"duration": duration,
									"lecture_id": r.head(1).lecture_id.values[0],
									"action": self.lecture_action})
			timestamp = pd.to_datetime(r.head(1).index.item())
			reduce_dict.update({"timestamp": timestamp, "username": user})

			video_events.append(reduce_dict)

		db = DataFrame(video_events)

		db.ix[db.action == self.lecture_action, 'video_tag'] = dispatch(db.lecture_id)

		return db
