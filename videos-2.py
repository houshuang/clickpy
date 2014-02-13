import pandas as pd
import numpy as np
from pandas import DataFrame, Series
import itertools
import time

def join_value(frame, store, values):
    for val in values:
        val_series = store[val].to_frame().reset_index().rename(columns={'index':val + '_val', 0: val})
        val_series = val_series.set_index(val)
        frame = frame.set_index(val)
        frame = frame.join(val_series, how='outer')
        frame = frame.reset_index()
    return(frame)

store = pd.HDFStore("mentalhealth_002.h5.repacked")

u = store.select('db', pd.Term('username = 127'))
# convert timestamps to something Pandas understands
# duration now will contain the length of the previous action, so we need
# to "shift it up" by one
u = u.reset_index()
u["timestamp_nice"]= pd.to_datetime(u.timestamp, unit='s')
u = u.sort(columns = 'timestamp')
u['duration'] = u.timestamp.diff()
u.duration = u.duration.shift(-1)

u2 = join_value(u, store, ['action', 'type', 'lecture_id'])
print(u2[['timestamp', 'timestamp_nice', 'duration', 'action_val', 'lecture_id_val', 'type_val']].sort("timestamp").head(20).to_string(header=False, index_names=False, na_rep=''))
