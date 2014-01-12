from tables import *

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

    def __len__(self):
        return(len(self.store))

    def toHash(self):
        return({v:k for k, v in self.store.items()})