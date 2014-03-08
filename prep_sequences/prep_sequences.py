"""Prep_sequences

Usage:
  prep_sequences.py <hdf-file> <out-file> [--cutoff TIMESTAMP] [--tmpdir DIR] [--procs N] [--force]

Arguments:
  <hdf-file>: Clicklog hdf file, must contain memoized data and lecture sequence
  <out-file>: Resulting file, path must exist

Options:
--cutoff TIMESTAMP       Only process events before given timestamp
--tmpdir DIR             Intermediate storage, default /tmp/clickpy, will be cleaned out [default: /tmp/clickpy]
--procs	N                Processors to use [default: 8]
--force                  Force overwriting all
"""

from docopt import docopt

import time
import pandas as pd
from subprocess import call, Popen
import numpy as np
from pandas import DataFrame, Series
import itertools
import sys
import os
import pickle
import pprint
from distutils.util import strtobool
import shutil
from uuid import uuid4
from action_converter import ActionConverter

pp = pprint.PrettyPrinter(indent=2).pprint

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
#**************************************************

arguments = docopt(__doc__)
python_exec = sys.executable
script_path = os.path.dirname(os.path.realpath(__file__))
hdffile = arguments['<hdf-file>']
outfile = arguments['<out-file>']
tmpdir = arguments['--tmpdir']
numprocesses = int(arguments['--procs'])
cutoff = arguments['--cutoff']
force = arguments['--force']

ensure_empty(tmpdir, force)
ensure_empty(outfile, force)
os.mkdir(tmpdir)

print("Spawning %d processing scripts" % numprocesses)
procs = []
for proc in range(1, numprocesses+1):
	range_start = proc
	range_jump = numprocesses+1
	procs.append(Popen([python_exec, "videos_reduce.py", hdffile, str(range_start), str(range_jump), tmpdir, cutoff]))

# wait for all processes to finish
while not all_procs_finished(procs):
	time.sleep(5)

print("All processes finished, concatenating files")
os.popen("cat %s/* > %s" % (tmpdir, outfile))
print("Processing completed")
