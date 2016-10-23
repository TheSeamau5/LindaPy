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


    # add host port [ ]
    # Triggered when received "[exec] add address.host address.port"
    def add_host(self, address):
        changes = self.nets_store.add(address)
        # resolve changes
        self._resolve_changes(changes)
        return None

    def _send_exec_add_host(self, address, remote_address):
        sock = socket.create_connection(remote_address)
        host, port = address
        message = "[exec] add {0} {1}".format(host, port)
        sock.send(message.encode('utf-8'))
        response = sock.recv(1024)
        sock.close()
        if not response:
            return None
        else:
            return response.decode('utf-8')


    # Request to join
    # Triggered when received "add B.host B.port"
    def request_to_join(self, address):
        print('Requesting to join: {0}'.format(address))
        sock = socket.create_connection(address)
        host, port = self.nets_store.local
        message = "[join] add {0} {1}".format(host, port)
        print('Sending request with message: {0}'.format(message))
        sock.send(message.encode('utf-8'))
        response = sock.recv(1024)
        sock.close()
        if not response:
            return None
        else:
            return response.decode('utf-8')

    # resolve request to join from some server [ ]
    # Triggered when received "[join] add B.host B.port"
    def resolve_request_to_join(self, address):
        print('Received request to join from: {0}'.format(address))
        # 1. Get list of all remote addresses
        remote_addresses = self.nets_store.get_remote_addresses()

        print('Broadcasting new addition to all addresses')
        # 2. Send "[exec] add B.host B.port"
        for remote_address in remote_addresses:
            self._send_exec_add_host(address, remote_address)

        print('Add the new address to the store')
        # 3. Add address to the nets store and get changes
        changes = self.nets_store.add(address)
        print('Get changes: {0}'.format(changes))

        print('Send table dump to new address')
        # 4. Dump new table on new address
        command = self._send_table_dump(address)

        print('Resolve changes')
        # 5. Resolve changes
        self._resolve_changes(changes)

        print('Resolved request to join')
        return command


    # send table [x]
    def _send_table_dump(self, address):
        print('Forming command')
        command = '[dump] add {0}'.format(' '.join(['{0} {1}'.format(x[0], x[1]) for x in self.nets_store.table]))
        return command
        # print('Creating socket connection')
        # sock = socket.create_connection(address)
        # print('Socket connection created')
        # print('Sending command')
        # sock.send(command.encode('utf-8'))
        # print('Command send and waiting for response')
        # #response = sock.recv(1024)
        # print('Response received')
        # print('Closing socket')
        # sock.close()
        # # print('Socket closed')
        # # if not response:
        # #     return None
        # # else:
        # #     return response.decode('utf-8')


    # dump table [x]
    # Triggered when received "[dump] add A.host A.port B.host B.port ..."
    def dump_table(self, table):
        self.nets_store.dump(table)
        return None


    # resolve changes [ ]
    def _resolve_changes(self, changes):
        for change in changes:
            slot, remote_address = change
            self.send_tuples(slot, remote_address)

    # Send tuples(that hash to a given slot) to some address
    def send_tuples(self, slot, address):
        tuples = self.tuple_store.remove_all(lambda t: hash_tuple(t) == slot)
        for t in tuples:
            self._insert_remote(t, address)


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



