import socket

from nets_store import *
from tuple_store import *
from hashing import hash_tuple


# Convert a description to a tuple string
def _description_to_tuple_string(description):
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


def _has_variable(description):
    return 'variable' in [x['type'] for x in description]


def _create_predicate(description):
    return lambda t: _match_description_to_tuple(description, t)


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
class Store:
    def __init__(self, session_name, local_address):
        self.nets_store = NetsStore(session_name, local_address)
        self.tuple_store = TupleStore(session_name)

    def _insert_remote(self, t, address):
        sock = socket.create_connection(address)
        message = "[exec] out{0}".format(t)
        sock.send(message.encode('utf-8'))
        response = sock.recv(1024)
        sock.close()
        if not response:
            return None
        else:
            return response.decode('utf-8')

    def insert(self, t):
        slot = hash_tuple(t)
        address = self.nets_store.table[slot]

        if address == self.nets_store.local:
            return self.tuple_store.insert(t)
        else:
            return self._insert_remote(t, address)

    def _read_remote(self, description, address):
        sock = socket.create_connection(address)
        t_str = _description_to_tuple_string(description)
        message = "[exec] rd{0}".format(t_str)
        sock.send(message.encode('utf-8'))
        response = sock.recv(1024)
        sock.close()
        if not response:
            return None
        else:
            return response.decode('utf-8')

    def read(self, description, local=True):
        if local:
            predicate = _create_predicate(description)
            return self.tuple_store.read(predicate)
        else:
            if _has_variable(description):
                # If there is a variable in the description
                # We simply have to broadcast the query
                addresses = self.nets_store.get_addresses()
                for address in addresses:
                    if address == self.nets_store.local:
                        predicate = _create_predicate(description)
                        t = self.tuple_store.read(predicate)
                    else:
                        t = self._read_remote(description, address)

                    if t is not None:
                        return t

                return None

            else:
                # Else, we have to hash the tuple and
                # Query the appropriate address
                query_tuple = tuple([x['value'] for x in description])
                slot = hash_tuple(query_tuple)
                address = self.nets_store.table[slot]

                if address == self.nets_store.local:
                    return self.tuple_store.read(lambda t: t == query_tuple)
                else:
                    return self._read_remote(description, address)

    def _remove_remote(self, description, address):
        sock = socket.create_connection(address)
        t_str = _description_to_tuple_string(description)
        message = "[exec] in{0}".format(t_str)
        sock.send(message.encode('utf-8'))
        response = sock.recv(1024)
        sock.close()
        if not response:
            return None
        else:
            return response.decode('utf-8')

    def remove(self, description, local=True):
        if local:
            predicate = _create_predicate(description)
            return self.tuple_store.remove(predicate)
        else:
            if _has_variable(description):
                # If there is a variable in the description
                # We simply have to broadcast the query
                addresses = self.nets_store.get_addresses()
                for address in addresses:
                    if address == self.nets_store.local:
                        predicate = _create_predicate(description)
                        t = self.tuple_store.remove(predicate)
                    else:
                        t = self._remove_remote(description, address)

                    if t is not None:
                        return t

                return None

            else:
                # Else, we have to hash the tuple and
                # Query the appropriate address
                query_tuple = tuple([x['value'] for x in description])
                slot = hash_tuple(query_tuple)
                address = self.nets_store.table[slot]

                if address == self.nets_store.local:
                    return self.tuple_store.remove(lambda t: t == query_tuple)
                else:
                    return self._remove_remote(description, address)



