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

from pandas import DataFrame, Series
import pandas as pd
import numpy as np

import memo

# fields that will be automatically memoized
memoizable_fields = ['action', 'page', 'type', 'visiting',
                     'key', 'session', 'username']
memos = {key:memo.Memo(key) for key in memoizable_fields}

# we'll strip off the nanosecond counter
# timestamp conversion is easier with Pandas afterwards
timestamp_fields = ['initTimestamp', 'eventTimestamp', 'timestamp']

# these are the columns that we are keeping, all others are
# removed
cols = ['action', 'currentTime', 'eventTimestamp', 'forum_id',
        'initTimestamp', 'key', 'lecture_id', 'page', 'paused',
        'playbackRate', 'post_id', 'prevTime', 'quiz_id', 'session',
        'submission_id', 'thread_id', 'timestamp', 'type', 'username']


def clean_number(nstr):
    try:
        a = int(not_num_re.sub('', nstr))
    except:
        a = np.NaN
    return(a)

def unwrap_hash(h):
    return( {k:v[0] for k,v in h.items()} )

def clean_value(v):
    if '#' in v:
        v = v.split('#')[0]
    return(v)


# precompiling regexps for speed
num_re = re.compile("\d")
not_num_re = re.compile('[^\d]')
human_grading_re = re.compile("human_grading/view/courses/\d+/assessments/\d+/?(.+?)?/*\d*$")
lecture_re = re.compile("lecture/(\d+)")

def parse(url, prefix_size):
    url, *part = url.strip().split('?', 1)

    # we remove the course prefix and everything after # from URL string
    # even if there is no "#", getting the first element will always
    # work
    actionstr = url[prefix_size:].split("#")[0]
    action = {}
    # first check if any numbers, to skip longer matches
    if re.search(num_re, actionstr):
        # special parsing for human grading actions
        match = re.match(human_grading_re, actionstr)
        if not match is None:
            actionstr = "human_grading/"
            if match.groups()[0]:
                actionstr = actionstr + match.groups()[0]
            return({'action': actionstr})

        # special parsing for lecture_views, otherwise proceed as normal
        match = re.search(lecture_re, actionstr)
        if not match is None:
            return({'action': 'lecture_view',
                    'lecture_id': match.groups()[0]})


    action = {'action': actionstr}

    if "human_grading" in action['action']:
        return({'action': 'human_grading/'})


    # process any url arguments (?arg=opt)
    if part:
        items = unwrap_hash(parse_qs(part[0]))
        items = {k:clean_value(v) for k,v in items.items()}
        action.update(items)

    return(action)

def get_prefix_size(fname):
    """Reads one line of the file to determine the prefix"""
    with open(fname) as f:
        firstline = f.readline()
        firstparse = ujson.loads(firstline)
        prefix = r"https://class.coursera.org/(.+?)/"
        return(len(re.match(prefix, firstparse['page_url']).group(0)))


def memoize(df):
    """Runs through list of fields to memoize, and returns
    new dataframe with all fields memoized"""
    for field, memoizer in memos.items():
        if field in df.columns:
            df[field] = df[field].apply(memoizer.get)
    return df


# flatten only once - ugly, what's a better way?
def flatten(items, onlyonce=True):
    for x in items:
        if onlyonce:
            yield from flatten(x, False)
        else:
            yield x


Npobj = np.dtype('O')

def storeappend(store, arr):
    if not arr or arr == []:
        return

    a = DataFrame(arr, columns=cols)
    a = memoize(a)

    # there should not be any strings left, but we need to clean the
    # numbers and convert to floats

    for col in a.columns:
        if a[col].dtype == Npobj:
            a[col] = a[col].apply(clean_number)
        a[col] = a[col].astype(np.float64)

    store.append('db', a, index=False)
    del a


def fix_timestamp(parsestr):
    """Remove last three digits (nanoseconds) from all timestamp fields"""
    for field in timestamp_fields:
        if field in parsestr:
            if isinstance(parsestr[field], int):
                parsestr[field] = parsestr[field]/1000
            elif isinstance(parsestr[field], str):
                parsestr[field] = parsestr[field][:-3]
    return(parsestr)


def process(prefix_size, line):
    parsestr = ujson.loads(line)

    if parsestr['key'] == "user.video.lecture.action":
        parsestr.update(ujson.loads(parsestr["value"]))

    urlparse = parse(parsestr['page_url'], prefix_size)
    parsestr.update(urlparse)
    parsestr = fix_timestamp(parsestr)
    return(parsestr)

# 4, 10,000 : 17
# 8, 10,000 : 17
# no effect of parallelization? too small chunks? try bigger chunks,
# or on server...

def proc_chunk(linearr, p, store):
    arr = p.map(procfunc, linearr)
    storeappend(store, arr)

def main(config):
    hdf = config.clicklog + (".h5")
    if os.path.exists(hdf):
        os.remove(hdf)

    store = pd.HDFStore(hdf, "w")

    p = Pool(config.pool_size)
    chunksize = config.chunk_size

    i = 0
    arr = []
    linearr = []

    with open(config.clicklog) as f:
        for line in f:
            i += 1
            linearr.append(line)

            if i / chunksize == i // chunksize:
                proc_chunk(linearr, p, store)
                linearr = []

                print(i)

        # if more lines left, process now
        if len(linearr) > 0:
            proc_chunk(linearr, p, store)
        del linearr

    print("Storing memoized data")
    for field, memoizer in memos.items():
        memoizer.store(store)

    store.close()
    print("Finished, now repacking with index")
    store = pd.HDFStore(config.clicklog + ".h5")
    db = store['db']
    db.to_hdf(config.clicklog + '.h5.repacked','db',mode='w',format='table',index=['timestamp'],
              data_columns=['action', 'username'])

    print("\nStatistics, unique values: (fields with M are memoized)")
    for col in db.columns:
        memoized = "M" if col in memoizable_fields else ""
        print(col, memoized, ": ", len(db[col].unique()))


def parse_args():
    parser = argparse.ArgumentParser(prog = 'clickpy',
        description='Process clicklog files')
    parser.add_argument('--pool-size', '-p', type=int, metavar='n', default=6,
        help="Size of pool for concurrent processing of lines (default 6)")
    parser.add_argument('--chunk-size', '-c', type=int, metavar='n', default=200000,
        help="Chunk of lines distributed amongst pools for each run (default 200,000)")
    parser.add_argument('--working-dir', '-d', type=str, metavar='x', default='/tmp/clickpy',
        help="Working workingdirectory, will be deleted on start (default /tmp/clickpy)")
    parser.add_argument('clicklog',type=str,
        help="clicklog file")

    return(parser.parse_args())


if __name__ == "__main__":
    config = parse_args()
    print("Starting...")
    prefix_size = get_prefix_size(config.clicklog)
    procfunc = partial(process, prefix_size)

    main(config)
