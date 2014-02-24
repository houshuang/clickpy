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

def ok_delete_p(question):
	return strtobool(input(question).lower())

def ensure_empty(f):
	if os.path.exists(f):
		if(ok_delete_p("%s exists, OK to delete?" % f)):
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

if len(sys.argv) < 5:
	print("Usage: log-file hdf-file tmp-dir num-processes prefix")
	exit()

logfile = sys.argv[1]
hdffile = sys.argv[2]
tmpdir = sys.argv[3]
dumpdir = sys.argv[3]+"/dump"
numprocesses = int(sys.argv[4])
prefix = sys.argv[5]

# make sure that paths are ready
ensure_empty(tmpdir)
ensure_empty(hdffile)
os.mkdir(tmpdir)
os.mkdir(tmpdir + "/sub")
os.mkdir(dumpdir)

# make sure that Redis keyspace is empty
r = redis.StrictRedis(host='localhost', decode_responses=True)
if r.exists(prefix + ":finished"):
	if(ok_delete_p("Keys with prefix %s exist, delete all?" % prefix)):
		r.delete(r.keys(prefix+":*"))
	else:
		exit()

print("Splitting log file into %s" % tmpdir)
call(['split', '-l 20', logfile], cwd=tmpdir)
store = pd.HDFStore(hdffile, "w")

print("Spawning %d processing scripts" % numprocesses)
procs = []
for proc in range(0, numprocesses):
	procs.append(Popen(["/Users/Stian/.py3/bin/python", "process.py", tmpdir, prefix]))

while True:
	t = perf_counter()
	print("Checking if any files")
	files = os.listdir(dumpdir)
	a = [f for f in files if os.path.isfile(os.path.join(dumpdir,f))]
	if a == []:
		if all_procs_finished(procs):
			break
		time.sleep(1)
		continue

	fname = os.path.join(dumpdir, a[0])
	arr = pickle.load(open(fname, "rb"))
	print(">>> Opening %s, %d units" % (a[0], len(arr)))

	store.append('db-video-proc', arr)
	os.remove(fname)
	print(">>> %s: %f" % (a[0], perf_counter()-t))

store.close()
print("### Finished")
