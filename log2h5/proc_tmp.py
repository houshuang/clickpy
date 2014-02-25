"""Log2h5

Usage:
  log2hf.py <log-file> <hdf-file> [--tmpdir DIR] [--procs N] [--redis-prefix PREFIX] [--line-chunks CHUNKS] [--force]

Arguments:
  <log-file>: Coursera log file
  <hdf-file>: Resulting hdf file, path must exist

Options:
--tmpdir DIR             Intermediate storage, default /tmp/clickpy, will be cleaned out [default: /tmp/clickpy]
--procs	N                Processors to use [default: 8]
--redis-prefix PREFIX    Redis-prefix to use, if running other programs simultaneously [default: log2hf]
--line-chunks CHUNKS     Line chunks to split log file [default: 2000000]
--force                  Force overwriting all (hdf5, tempdir and redis-keys)
"""

from docopt import docopt
from time import perf_counter
import os
import time
import pickle
import sys
from pandas import DataFrame
import pandas as pd
import numpy as np
import redis
from distutils.util import strtobool
from subprocess import call, Popen
import shutil
import memo

def ok_delete_p(question):
	return strtobool(input(question).lower())

def ensure_empty(f, force):
	if os.path.exists(f):
		if force or ok_delete_p("%s exists, OK to delete?" % f):
			if os.path.isdir(f):
				shutil.rmtree(f)
			else:
				os.remove(f)
			return(True)
		else:
			return(False)
	else:
		return(True)

def all_procs_finished(procs):
	for proc in procs:
		if proc.poll() == None:
			return False
	return True

arguments = docopt(__doc__)
python_exec = sys.executable
script_path = os.path.dirname(os.path.realpath(__file__))
logfile = arguments['<log-file>']
hdffile = arguments['<hdf-file>']
tmpdir = arguments['--tmpdir']
dumpdir = tmpdir + "/dump"
numprocesses = int(arguments['--procs'])
prefix = arguments['--redis-prefix']
splitlines = arguments['--line-chunks']
force = arguments['--force']

memoizable_fields = ['action', 'page', 'type', 'visiting',
						  'key', 'session', 'username']
memos = {key:memo.Memo(key, prefix) for key in memoizable_fields}

# make sure that paths are ready
ensure_empty(tmpdir, force)
ensure_empty(hdffile, force)
os.mkdir(tmpdir)
os.mkdir(tmpdir + "/sub")
os.mkdir(dumpdir)

# make sure that Redis keyspace is empty
r = redis.StrictRedis(host='localhost', decode_responses=True)
if r.exists(prefix + ":finished"):
	if force or ok_delete_p("Keys with prefix %s exist, delete all?" % prefix):
		r.delete(r.keys(prefix+":*"))
	else:
		exit()

print("Splitting log file into %s" % tmpdir)
r.set(prefix + ":split-finished", "0")
splitfinished = False

split = Popen(['split', '-a 5', '-l %s' % splitlines, os.path.join(script_path, logfile)], cwd=tmpdir)
store = pd.HDFStore(hdffile, "w")

print("Spawning %d processing scripts" % numprocesses)
procs = []
for proc in range(0, numprocesses):
	procs.append(Popen([python_exec, "process.py", tmpdir, prefix]))

while True:
	t = perf_counter()

	if (not splitfinished) and split.poll() is not None:
		print("Splitting finished, sending signal")
		r.set(prefix + ":split-finished", "1")
		splitfinished = True

	files = os.listdir(dumpdir)
	a = [f for f in files if os.path.isfile(os.path.join(dumpdir,f))]
	if a == []:
		if all_procs_finished(procs):
			break
		time.sleep(2)
		continue

	fname = os.path.join(dumpdir, a[0])
	arr = pickle.load(open(fname, "rb"))
	print(">>> Opening %s, %d units" % (a[0], len(arr)))

	store.append('db', arr)
	os.remove(fname)
	print(">>> %s: %f" % (a[0], perf_counter()-t))

print("Processing finished, memoizing data")
for field, memoizer in memos.items():
	store[field] = memoizer.to_series()

store.close()
shutil.rmtree(tmpdir)
r.delete(r.keys(prefix+":*"))

print("### Finished")
