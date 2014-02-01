import os
import re
import sys
import ujson
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

# precompiling regexps for speed
num_re = re.compile("\d")
not_num_re = re.compile('[^\d]')
human_grading_re = re.compile("human_grading/view/courses/\d+/assessments/\d+/(.+?)/\d*")
lecture_re = re.compile("lecture/(\d+)")

def clean_number(nstr):
    try:
        a = int(notnumber.sub('', nstr))
    except:
        a = np.NaN
    return(a)

def unwrap_hash(h):
    return( {k:v[0] for k,v in h.items()} )

def clean_value(v):
    if '#' in v:
        v = v.split('#')[0]
    return(v)


def parse(url, prefix_size):
    url, *part = url.strip().split('?', 1)

    # we remove the course prefix and everything after # from URL string
    # even if there is no "#", getting the first element will always
    # work
    actionstr = url[prefix_size:].split("#")[0]

    # first check if any numbers, to skip longer matches
    if re.match(num_re, actionstr):

        # special parsing for human grading actions
        match = re.match(human_grading_re, actionstr)
        if not match is None:
            actionstr = "human_grading/" + match.groups()[0]
            action = {'action': actionstr}

        # special parsing for lecture_views, otherwise proceed as normal
        match = re.match(lecture_re, actionstr)
        if not match is None:
            action = {'action': 'lecture_view',
                      'lecture_id': match.groups()[0]}
        else:
            print("Uncaught numeric action: ", actionstr)

    else:
        action = {'action': actionstr}

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

    a = list(flatten(arr))
    a = DataFrame(a, columns=cols)
    a = memoize(a)

    # there should not be any strings left, but we need to clean the
    # numbers and convert to floats
    for col in a.columns:
        if a[col].dtype == Npobj:
            a[col] = a[col].apply(clean_number)
            a[col] = a[col].astype(np.float64)

    store.append('db', a)
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


def process(prefix_size, linechunk):
    arr = []
    for line in linechunk:
        if not linechunk or linechunk == [] or len(linechunk) == 0:
            next
        parsestr = ujson.loads(line)

        if parsestr['key'] == "user.video.lecture.action":
            parsestr.update(ujson.loads(parsestr["value"]))

        urlparse = parse(parsestr['page_url'], prefix_size)
        parsestr.update(urlparse)
        parsestr = fix_timestamp(parsestr)
        arr.append(parsestr)
    return(arr)

# 4, 10,000 : 17
# 8, 10,000 : 17
# no effect of parallelization? too small chunks? try bigger chunks,
# or on server...

def main(fname, test):
    prefix_size = get_prefix_size(fname)
    procfunc = partial(process, prefix_size)

    arr = []
    hdf = fname+(".h5")

    store = pd.HDFStore(hdf, "w")
    p = Pool(4)
    with open(fname) as f:
        i = 0
        while True:
            i += 1
            lines = list(islice(f, 200000))
            if not lines or lines == []:
                break
            linechunks = [list(islice(lines, 50000)),list(islice(lines, 50000)),
                list(islice(lines, 50000)),list(islice(lines, 50000))]

            arr = p.map(procfunc, linechunks)
            storeappend(store, arr)

            print(i*200000)
            arr = []
            if test:
                break

    print("Storing memoized data")
    for field, memoizer in memos.items():
        memoizer.store(store)

    store.close()
    print("Finished")

if __name__ == "__main__":

    print("Starting...")
    if len(sys.argv) > 1:
        fname = sys.argv[1]
    if len(sys.argv) > 2:
        test = True
    else:
        test = False

    main(fname, test)
