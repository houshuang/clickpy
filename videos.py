import pandas as pd
import numpy as np
from pandas import DataFrame, Series
import itertools

store = pd.HDFStore("mentalhealth_002.h5.repacked")

# user 426 has 10k events

u = store.select('db', pd.Term('username = 127'))

# convert timestamps to something Pandas understands
# duration now will contain the length of the previous action, so we need
# to "shift it up" by one

def join_value(frame, store, values):
    for val in values:
        val_series = store[val].to_frame().reset_index().rename(columns={'index':val + '_val', 0: val})
        val_series = val_series.set_index(val)
        frame = frame.set_index(val)
        frame = frame.join(val_series, how='outer')
        frame = frame.reset_index()
    return(frame)

u = u.reset_index()
u.timestamp = pd.to_datetime(u.timestamp, unit='s')
u = u.sort(columns = 'timestamp')
u['duration'] = u.timestamp.diff()
u.duration = u.duration.shift(-1)
u['indexcounter'] = Series(list(range(0,len(u.index))))
print(u[['indexcounter', 'timestamp']])
print(len(u.timestamp.unique()))
exit()
u.reindex(index=['indexcounter'], inplace=True)
u.reset_index()
tuples = [tuple(x) for x in u[['timestamp', 'action', 'lecture_id', 'indexcounter']].to_records(index=False)]

video_sections = [list(g) for k,g in itertools.groupby(tuples, lambda x: (x[0], x[1]))]

u = join_value(u, store, ['action', 'type', 'lecture_id'])
u.set_index('indexcounter')
for segment in video_sections:
    rows = []
    for event in segment:
        rows.append(event[3])

    print(rows)
    r = u.loc[rows]
    print(r.ix[:,['indexcounter','duration','action_val', 'lecture_id_val', 'type_val']])
