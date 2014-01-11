import re
import os
from pandas import DataFrame, Series
import pandas as pd
import numpy as np
import sys
import ujson
from urllib.parse import parse_qs
import pytables

C = 0

def unwrap_hash(h):
    return( {k:v[0] for k,v in h.items()} )

def parse(url):
    print(url.strip())

    url, *part = url.strip().split('?', 1)
    action = {'action': url}

    if part:
        action.update(unwrap_hash(parse_qs(part[0])))

    return(action)


class Memo(object):
    """Gives back the index number if object already exists, or stores
    and returns index

    Example:
    a = Memo()
    a['http://reganmian.net'] # => 1
    a['http://google.com'] # => 2
    a['http://reganmian.net'] # => 1
    """

    def __init__(self):
        self.store = {}
        self.counter = 0

    def __getitem__(self, s):
        if s in self.store:
            return self.store[s]
        else:
            self.counter += 1
            self.store[s] = self.counter
            return self.counter

    def len(self):
        return(len(self.store))

if __name__ == "__main__":

    print("Starting...")

    with open("/Users/Stian/src/pythonstuff/clicklog.txt") as f:
        for line in f:
            print(ujson.loads(line))
            print(sorted(parse(line).items()),"\n")
            exit()

#[parse(line) for line in open("/Users/Stian/src/pythonstuff/urls5")]
