from time import perf_counter
import os
import time
import pickle
import sys
from pandas import DataFrame
import pandas as pd
import numpy as np

if len(sys.argv) < 3:
	print("Usage: tmpdir storepath")
	exit()

tmpdir = sys.argv[1]
storepath = sys.argv[2]
if os.path.exists(storepath):
    os.remove(storepath)
store = pd.HDFStore(storepath, "w")

while True:
    t = perf_counter()

    files = os.listdir(tmpdir)
    a = [f for f in files if os.path.isfile(os.path.join(tmpdir,f))
        and f != "finished.signal"]
    if a == []:
        if "finished.signal" in files:
            break
        time.sleep(1)
        continue
    fname = os.path.join(tmpdir, a[0])
    arr = pickle.load(open(fname, "rb"))
    print(">>> Opening %s, %d units" % (a[0], len(arr)))

    store.append('db-video-proc', arr)
    os.remove(fname)
    print(">>> %s: %f" % (a[0], perf_counter()-t))

os.remove(os.path.join(tmpdir, "finished.signal"))

store.close()
print("### Finished")
