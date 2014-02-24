import redis
import pandas as pd
import numpy as np
from functools import lru_cache

class Memo(object):
	"""Gives back the index number if object already exists, or stores
	and returns index

	Example:
	a = Memo()
	a['http://reganmian.net'] # => 1
	a['http://google.com'] # => 2
	a['http://reganmian.net'] # => 1
	"""

	def __init__(self, name, prefix):
		self.r = redis.StrictRedis(host='localhost', decode_responses=True)
		self.name = name
		self.prefix = prefix
		self.counter = '%s:%s:cnt' % (prefix, name)
		self.store = '%s:%s:store' % (prefix, name)
		if self.r.zcard(self.store) == 0:
			self.add_object('None')

	def add_object(self, obj):
		c = self.r.incr(self.counter)
		self.r.zadd(self.store, c, obj)
		return c

	@lru_cache(maxsize=None)
	def get(self, s):
		if s is None or pd.isnull(s):
			return -1

		existing_score = self.r.zscore(self.store, s)
		if existing_score:
			return existing_score
		else:
			c = self.add_object(s)
			return c

	def __getitem__(self, s):
		return(self.get(s))

	def __len__(self):
		return(self.r.zcard(self.store))

	def to_series(self):
		vals = self.r.zrange(self.store, 0, -1, withscores=True, score_cast_func = int)
		return pd.Series(dict(vals))
