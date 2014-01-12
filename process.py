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

C = 0

unwanted_fields = ['data'] 
memoizable_fields = ['action', 'page', 'quiz_id', 'type', 'visiting']

memos = {key:memo.Memo() for key in memoizable_fields}

def unwrap_hash(h):
    return( {k:v[0] for k,v in h.items()} )

def parse(url, prefix_size):

    url, *part = url.strip().split('?', 1)
    action = {'action': url[prefix_size:]}

    if part:
        action.update(unwrap_hash(parse_qs(part[0])))

    # remove fields we don't care about
    for field in unwanted_fields:
        if field in action: del action[field]

    # memoize fields
    for field, memoizer in memos.items():
        if field in action: 
            action[field] = memoizer[action[field]]

    return(action)

def get_prefix_size(fname):
    """Reads one line of the file to determine the prefix"""
    with open(fname) as f:
        firstline = f.readline()
        firstparse = ujson.loads(firstline)
        prefix = r"https://class.coursera.org/(.+?)/"
        return(len(re.match(prefix, firstparse['page_url']).group(0)))


def main(fname):
    
    prefix_size = get_prefix_size(fname)

    with open(fname) as f:
        for line in f:
            parsestr = ujson.loads(line)
            if parsestr['key'] != "pageview":
                continue
            urlparse = parse(parsestr['page_url'], prefix_size)
            print(urlparse)

    for field, memoizer in memos.items():
        print(field)
        print(memoizer.toHash())



if __name__ == "__main__":

    print("Starting...")
    fname = "/Users/Stian/Dropbox/Laurie/Enc/clicklog-prog2-1k"

    main(fname)