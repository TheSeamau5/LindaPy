class TupleStore:
    def __init__(self):
        self.tuples = []

    def insert(self, t):
        self.tuples.append(t)

    def read(self, predicate):
        try:
            result = next(t for t in self.tuples if predicate(t))
            return result
        except:
            return None

    def remove(self, predicate):
        try:
            result = next(t for t in self.tuples if predicate(t))
            self.tuples.remove(result)
            return result
        except:
            return None

    def read_all(self, predicate):
        return [x for x in self.tuples if predicate(x)]

    def remove_all(self, predicate):
        result = [x for x in self.tuples if predicate(x)]
        self.tuples = [x for x in self.tuples if not predicate(x)]
        return result