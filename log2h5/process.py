import pickle
from uuid import uuid4
import os
import re
import sys
import ujson
import argparse
from collections import Counter, Iterable
from datetime import datetime
from functools import partial
from itertools import islice
from multiprocessing import Pool
from urllib.parse import parse_qs
import time
from time import perf_counter

from pandas import DataFrame, Series
import pandas as pd
import numpy as np
import redis
import memo

class Logger(object):
	def __init__(self, r, prefix):
		self.r = r
		self.logstore = '%s:log' % prefix

	def log(self, text):
		self.r.rpush(self.logstore, text)

class Process(object):
	def get_prefix_size(self, fname):
		"""Reads one line of the file to determine the prefix"""
		with open(fname) as f:
			firstline = f.readline()
			firstparse = ujson.loads(firstline)
			prefix = r"https://class.coursera.org/(.+?)/"
			return(len(re.match(prefix, firstparse['page_url']).group(0)))

	def __init__(self, argv):
		if len(argv) < 2:
			print("Usage: python process.py tmpdir prefix")
			exit()

		self.tmpdir = argv[1]
		self.prefix = argv[2]
		self.prefix_size = None

		# fields that will be automatically memoized
		self.memoizable_fields = ['action', 'page', 'type', 'visiting',
							 'key', 'session', 'username']
		self.memos = {key:memo.Memo(key, self.prefix) for key in self.memoizable_fields}

		# we'll strip off the nanosecond counter
		# timestamp conversion is easier with Pandas afterwards
		self.timestamp_fields = ['initTimestamp', 'eventTimestamp', 'timestamp']

		# these are the columns that we are keeping, all others are
		# removed
		self.cols = ['action', 'currentTime', 'eventTimestamp', 'forum_id',
				'initTimestamp', 'key', 'lecture_id', 'page', 'paused',
				'playbackRate', 'post_id', 'prevTime', 'quiz_id', 'session',
				'submission_id', 'thread_id', 'timestamp', 'type', 'username']

		# precompiling regexps for speed
		self.num_re = re.compile("\d")
		self.not_num_re = re.compile('[^\d]')
		self.human_grading_re = re.compile("human_grading/view/courses/\d+/assessments/\d+/?(.+?)?/*\d*$")
		self.lecture_re = re.compile("lecture/(\d+)")

		self.pid = os.getpid()
		self.r = redis.StrictRedis(host='localhost', decode_responses=True)
		self.logger = Logger(self.r, self.prefix)
		self.npobj = np.dtype('O')

	def clean_number(self, nstr):
		try:
			a = int(self.not_num_re.sub('', nstr))
		except:
			a = np.NaN
		return(a)

	def unwrap_hash(self, h):
		return( {k:v[0] for k,v in h.items()} )

	def clean_value(self, v):
		if '#' in v:
			v = v.split('#')[0]
		return(v)

	def parse(self, url):
		url, *part = url.strip().split('?', 1)

		# we remove the course prefix and everything after # from URL string
		# even if there is no "#", getting the first element will always
		# work
		actionstr = url[self.prefix_size:].split("#")[0]
		action = {}
		# first check if any numbers, to skip longer matches
		# try:
		if re.search(self.num_re, actionstr):
			# special parsing for human grading actions
			match = re.match(self.human_grading_re, actionstr)
			if not match is None:
				actionstr = "human_grading/"
				return({'action': actionstr})

			# special parsing for lecture_views, otherwise proceed as normal
			match = re.search(self.lecture_re, actionstr)
			if not match is None:
				return({'action': 'lecture_view',
						'lecture_id': match.groups()[0]})


		action = {'action': actionstr}

		if "human_grading" in action['action']:
			return({'action': 'human_grading/'})


		# process any url arguments (?arg=opt)
		if part:
			items = self.unwrap_hash(parse_qs(part[0]))
			items = {k:self.clean_value(v) for k,v in items.items()}
			action.update(items)

		return(action)

	def memoize(self, df):
		"""Runs through list of fields to memoize, and returns
		ew dataframe with all fields memoized"""
		for field, memoizer in self.memos.items():
			if field in df.columns:
				df[field] = df[field].apply(memoizer.get)
		return df


	# flatten only once - ugly, what's a better way?
	def flatten(self, items, onlyonce=True):
		for x in items:
			if onlyonce:
				yield from flatten(x, False)
			else:
				yield x

	def dump(self, arr):
		"""Dumps lines to a text file in tmp dir"""

		idstr = str(uuid4()) + "=" + str(self.pid)
		tmpname = os.path.join(self.tmpdir, "sub", idstr)
		dumpname = os.path.join(self.tmpdir, "dump", idstr)
		with open(tmpname, "wb") as dumpf:
			pickle.dump(arr, dumpf)
		os.rename(tmpname, dumpname)

		print("*** %d: Dumping to %s" % (self.pid, idstr))

	def storeappend(self, arr):
		if not arr or arr == []:
			return

		a = DataFrame(arr, columns=self.cols)
		a = self.memoize(a)

		# there should not be any strings left, but we need to clean the
		# numbers and convert to floats

		for col in a.columns:
			if a[col].dtype == self.npobj:
				a[col] = a[col].apply(self.clean_number)
			a[col] = a[col].astype(np.float64)

		self.dump(a)

	def fix_timestamp(self, parsestr):
		"""Remove last three digits (nanoseconds) from all timestamp fields"""
		for field in self.timestamp_fields:
			if field in parsestr:
				if isinstance(parsestr[field], int):
					parsestr[field] = parsestr[field]/1000
				elif isinstance(parsestr[field], str):
					parsestr[field] = parsestr[field][:-3]
		return(parsestr)

	def process(self, line):
		parsestr = ujson.loads(line)

		if parsestr['key'] == "user.video.lecture.action":
			parsestr.update(ujson.loads(parsestr["value"]))

		urlparse = self.parse(parsestr['page_url'])
		parsestr.update(urlparse)
		parsestr = self.fix_timestamp(parsestr)
		return(parsestr)


	def start(self):
		# do as long as there are still files to process

		while True:
			files = os.listdir(self.tmpdir)
			a = [f for f in files if os.path.isfile(os.path.join(self.tmpdir, f))]
			if a == []:
				if self.r.get(self.prefix+":split-finished") == "0":
					time.sleep(5)
					continue
				else:
					break # done processing

			nextfile = a[0]
			fname = os.path.join(self.tmpdir, nextfile) # grab the first file we see
			fname_sub = os.path.join(self.tmpdir, "sub", nextfile)

			try:
				os.rename(fname, fname_sub) # move it, to avoid others getting their hands on it
			except:
				continue # someone else grabbed the file, try again

			print(">>> %d: Opening %s" % (self.pid, nextfile))
			t = perf_counter()

			if not self.prefix_size:
				self.prefix_size = self.get_prefix_size(fname_sub)

			proclines = []
			with open(fname_sub) as f:
				for line in f:
					try:
						proclines.append(self.process(line))
					except:
						self.logger.log("Failed parse: %s" % line)

			self.storeappend(proclines)
			os.remove(fname_sub)
			print(">>> %d: Done with %s (%f)" % (self.pid, nextfile, perf_counter()-t))

		# no more files, send sentinel and wrap up
		self.r.rpush('%s:finished' % (self.prefix) , self.pid)
		print(">>> %d: Finished with all files, shutting down" % self.pid)

if __name__ == "__main__":
	print("Starting...")
	proc = Process(sys.argv)
	proc.start()
