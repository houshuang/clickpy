import pytables

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
