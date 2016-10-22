# Actions:
# add IP port
# remove IP port
# out(...)
# rd(...)
# in(...)
# [exec] out(...)
# [exec] rd(...)
# [exec] in(...)
# Request to join  [join] add IP port
# Dump table  [dump] add IP1 port1 IP2 port2 ...
from parser import parse


class Interpreter:
    def __init__(self, store):
        self.store = store

    def evaluate(self, tree):
        # if tree['command'] == 'add':
        #     for arg in tree['args']:
        #         address = (arg['host'], arg['port'])
        #         return self.nets_store.add(address)

        # elif tree['command'] == 'remove':
        #     for arg in tree['args']:
        #         address = (arg['host'], arg['port'])
        #         return self.nets_store.remove(address)

        if tree['command'] == 'out':
            t = []

            for item in tree['args']:
                if item['type'] == 'value':
                    t.append(item['value'])
                else:
                    print('"out" only accepts value tuples')
                    return None

            t = tuple(t)

            return self.store.insert(t)

        elif tree['command'] == 'rd':
            description = tree['args']

            return self.store.read(description, local=True)

        elif tree['command'] == 'in':
            description = tree['args']

            return self.store.remove(description, local=True)

    def interpret(self, command):
        print('Interpreting command: {0}'.format(command))
        try:
            tree = parse(command)
            print('Tree: {0}'.format(tree))
            return self.evaluate(tree)

        except Exception as e:
            print('Error: {0}'.format(e))
            return None
