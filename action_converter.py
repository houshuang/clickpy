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

	def proc_sequence(self, row):
		l = row['lecture_id']
		if self.seen == []:
			self.seen.append(l)
			return None # no tag
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

	def proc(self, row):
		seq = self.proc_sequence(row)
		tags = []
		if seq:
			tags.append(seq)
		for x in ['seeked', 'pause', 'ratechange']:
			if x in row:
				tags.append(x)
		return tags

def dispatch(vals):
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

def get_index_by_value(pdseries, value):
	"Looks up Pandas.Series index by value"
	return(pdseries[pdseries == value].index[0])

class ActionConverter(object):
	def __init__(self, store):
		self.store = store
		self.store_length = store['db'].username.max()
		self.lecture_action = store['action']['lecture/view']
		self.handlers = {'lecture/view': LectureView}

	def convert(self, user, max_time = None):
		store = self.store
		term = "username = %s" % user
		if max_time:
			term += " & timestamp < %d" % max_time
		u = store.select('db', pd.Term(term))
		username = get_index_by_value(self.store['username'], user)

		handlers = {k:v() for k,v in self.handlers.items()} # initialize handlers

		# checking if there are any events
		if len(u) == 0:
			return(None)

		u = u.reset_index()
		u = u.sort(columns = 'timestamp')
		u['duration'] = u.timestamp.diff()
		u.duration = u.duration.shift(-1)
		tuples = [tuple(x) for x in u[['timestamp', 'action', 'lecture_id']].
				  to_records(index=False)]
		video_sections = [list(g) for k,g in itertools.
						  groupby(tuples, lambda x: (x[1], x[2]))]

		u = join_value(u, store, ['type', 'action'])
		video_events = []
		u = u.set_index('timestamp')
		for segment in video_sections:
			# **************************************************
			# first, reduce multi-line video events to one line and pass through other events
			# into a dict, which we can further process
			rows = []
			for event in segment:
				rows.append(event[0])

			r = u.loc[rows]

			# if not a video event, just write it in
			if r.head(1).action.values[0] != self.lecture_action:
				reduce_dict = r.reset_index().to_dict(outtype='records')[0]

			# if a video event, reduce down to one line
			else:
				reduce_dict = r.type_val.value_counts().to_dict()
				duration = r.duration.sum()
				timestamp = r.head(1).index.item()
				action_val = r.head(1).action_val.values[0]
				reduce_dict.update({"duration": duration,
									"lecture_id": r.head(1).lecture_id.values[0],
									"action": self.lecture_action,
									"username": user,
									"timestamp": timestamp,
									"action_val": action_val})

			# ************************************************
			# parse video events depending on action_type, and add tags
			tags = []
			if reduce_dict['action_val'] in handlers.keys():
				tags = handlers[reduce_dict['action_val']].proc(reduce_dict)
			if reduce_dict['duration'] < 3:
				tags.append('short-event')
			reduce_dict['tags'] = tags

			video_events.append(reduce_dict)

		# **************************************************
		# Formatting for arules-sequence

		txt = ''
		for event in video_events:
			tagcnt = 1
			if 'tags' in event:
				tagcnt += len(event['tags'])
				tags = ' '.join(event['tags'])
			else:
				tags = ''
			line = "%s %s %d %s %s" % (username, event['timestamp'], tagcnt, event['action_val'], tags)
			txt += line + "\n"

		return (txt)
