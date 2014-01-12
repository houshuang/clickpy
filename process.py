import re
import sys
import os
from pandas import DataFrame, Series
import pandas as pd
import numpy as np
import sys
import ujson
from urllib.parse import parse_qs
import memo
from collections import Counter
from datetime import datetime

C = 0
ipy
unwanted_fields = ['data', 'user_ip', '12', '13', 'language', 'from',
    'user_agent', 12, 13, 'page_url', 'client', 'networkState',
    'visiting', 'error', 'lecture_player', 'minimal', 'readyState',
    'user_id', 'value'] 
memoizable_fields = ['action', 'page', 'quiz_id', 'type', 'visiting',
    'key', 'session', 'username']
timestamp_fields = ['initTimestamp', 'eventTimestamp', 'timestamp']

memos = {key:memo.Memo() for key in memoizable_fields}

db = DataFrame()

def unwrap_hash(h):
    return( {k:v[0] for k,v in h.items()} )

def parse(url, prefix_size):

    url, *part = url.strip().split('?', 1)
    action = {'action': url[prefix_size:]}

    if part:
        action.update(unwrap_hash(parse_qs(part[0])))

    return(action)

def get_prefix_size(fname):
    """Reads one line of the file to determine the prefix"""
    with open(fname) as f:
        firstline = f.readline()
        firstparse = ujson.loads(firstline)
        prefix = r"https://class.coursera.org/(.+?)/"
        return(len(re.match(prefix, firstparse['page_url']).group(0)))

def memoize(action):
    # remove fields we don't care about
    for field in unwanted_fields:
        if field in action: del action[field]

    # memoize fields
    for field, memoizer in memos.items():
        if field in action: 
            action[field] = memoizer[action[field]]
    return action

def nan_or_timestamp(val):
    if not (pd.isnull(val)):
        val = datetime.fromtimestamp(int(val) / 1000.)
    return val

def inject_df(vals):
    db.join()

def main(fname):
    
    prefix_size = get_prefix_size(fname)

    arr = []
    with open(fname) as f:
        for i, line in enumerate(f):
            parsestr = ujson.loads(line)

            if parsestr['key'] == "user.video.lecture.action":
                parsestr.update(ujson.loads(parsestr["value"]))

            urlparse = parse(parsestr['page_url'], prefix_size)
            parsestr.update(urlparse)

            arr.append(memoize(parsestr))

            if (i/1000 == i//1000):
                db.append(DataFrame(arr), ignore_index=True)
                arr = []

    # convert epoch in milliseconds to datetime
    for col in timestamp_fields:
        db[col] = db[col].apply(nan_or_timestamp)
    

    for col in db.columns:
        print("%s: %i, %i" % (col, db[col].count(), len(db[col].unique())))
    return(db)


if __name__ == "__main__":

    print("Starting...")
    fname = "/Users/Stian/src/clickpy/introstats_001_10k"

    db=main(fname)