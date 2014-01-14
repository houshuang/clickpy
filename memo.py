import pandas as pd

# class MemoObject(IsDescription):
#     index = UInt16Col()
#     content = StringCol(200)

# h5file = openFile("test.h5", mode="w+", title="Test")
# table = h5file.createTable(h5file.root, "test", MemoObject, "Test example")

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
        self.db = {}
        self.counter = 0
        self.name = name

    def __getitem__(self, s):
        if s in self.db:
            return self.db[s]
        else:
            self.counter += 1
            self.db[s] = self.counter
            return self.counter

    def __len__(self):
        return(len(self.db))

    def toHash(self):
        return({v:k for k, v in self.db.items()})

    def store(self, store):
        store[self.name] = pd.Series(list(self.db.keys()), list(self.db.values()))