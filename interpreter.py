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
import socket

from parser import parse


# Convert a description to a tuple string
def description_to_tuple_string(description):
    items = []
    print('Description: {0}'.format(description))
    for item in description:
        if item['type'] == 'value':
            value = item['value']

            kind = value.__class__.__name__
            if kind == 'str':
                value = '"{0}"'.format(value)

            items.append(value)

        elif item['type'] == 'variable':
            items.append('?{0}:{1}'.format(item['name'], item['kind']))

    items = [str(x) for x in items]

    print('Items: {0}'.format(items))

    return '({0})'.format(','.join(items))

def _match_description_to_tuple(description, t):
    if len(description) == len(t) == 0:
        return True

    if len(description) == len(t):
        for i in range(len(description)):
            item = description[i]
            t_item = t[i]

            if item['type'] == 'value':
                if not item['value'] == t_item:
                    return False

            elif item['type'] == 'variable':
                t_item_kind = t_item.__class__.__name__
                if not item['kind'] == t_item_kind:
                    return False

            else:
                return False

        return True

    else:
        return False


class Interpreter:
    def __init__(self, nets_store, tuple_store):
        self.nets_store = nets_store
        self.tuple_store = tuple_store

    # Read locally
    def _evaluate_read_locally(self, description):
        def predicate(t):
            return _match_description_to_tuple(description, t)

        return self.tuple_store.read(predicate)

    # Read remotely
    def _evaluate_read_remotely(self, description, address):
        sock = socket.create_connection(address)
        message = "[exec] rd{0}".format(description_to_tuple_string(description))
        sock.send(message.encode('utf-8'))
        response = sock.recv(1024)
        sock.close()
        if not response:
            return None
        else:
            return response.decode('utf-8')

    #def _evaluate_read(self, description):
        # First try locally
        #t


    def evaluate(self, tree):
        if tree['command'] == 'add':
            for arg in tree['args']:
                address = (arg['host'], arg['port'])
                return self.nets_store.add(address)

        elif tree['command'] == 'remove':
            for arg in tree['args']:
                address = (arg['host'], arg['port'])
                return self.nets_store.remove(address)

        elif tree['command'] == 'out':
            t = []
            for item in tree['args']:
                if item['type'] == 'value':
                    t.append(item['value'])
                else:
                    print('"out" only accepts value tuples')
                    return None
            t = tuple(t)
            return self.tuple_store.insert(t)

        elif tree['command'] == 'rd':
            description = tree['args']

            def predicate(t):
                return _match_description_to_tuple(description, t)

            return self.tuple_store.read(predicate)

        elif tree['command'] == 'in':
            description = tree['args']

            def predicate(t):
                return _match_description_to_tuple(description, t)

            return self.tuple_store.remove(predicate)

    def interpret(self, command):
        print('Interpreting command: {0}'.format(command))
        try:
            tree = parse(command)
            print('Tree: {0}'.format(tree))
            return self.evaluate(tree)

        except Exception as e:
            print('Error: {0}'.format(e))
            return None
