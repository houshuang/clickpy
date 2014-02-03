import pandas as pd
import numpy as np
from pandas import DataFrame, Series

store = pd.HDFStore("mentalhealth_002.h5.repacked")

# user 426 has 10k events

u = store.select('db', pd.Term('username = 127'))

# convert timestamps to something Pandas understands
# duration now will contain the length of the previous action, so we need
# to "shift it up" by one

u.timestamp = pd.to_datetime(u.timestamp, unit='s')
u.duration = u.timestamp.diff()
u.duration = u.duration.shift(-1)

# we need to extract the video events and group them

videokey = store['key']['user.video.lecture.action']
v = u[u.key==videokey]
print(u.action.unique())
print(v.action.unique())
print(store['action'])
print(u.columns)
print(store['type'])
print(v.type.unique())
