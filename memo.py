import pandas as pd
import numpy as np

class Memo(object):
    """Gives back the index number if object already exists, or stores
    and returns index

    Example:
    a = Memo()
    a['http://reganmian.net'] # => 1
    a['http://google.com'] # => 2
    a['http://reganmian.net'] # => 1
    """

    def __init__(self, name):
        self.db = pd.Series()
        self.counter = 0
        self.name = name

    def get(self, s):
        if s is None or pd.isnull(s):
            return -1

        item = self.db.get(s)
        if item:
            return item
        else:
            self.counter += 1
            self.db[s] = self.counter
            return self.counter

    def __getitem__(self, s):
        return(self.get(s))

    def __len__(self):
        return(len(self.db))

    def store(self, store):
        store[self.name] = self.db
