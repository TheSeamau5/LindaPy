import socket
import zlib

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
# Update address update old_IP old_port new_IP new_port
# Dump table  [dump] add IP1 port1 IP2 port2 ...
class Store:
    def __init__(self, session_name, local_address):
        self.nets_store = NetsStore(session_name, local_address)
        self.tuple_store = TupleStore(session_name)

    def _send_receive_to_address(self, message, address):
        # 1. Encode message to utf-8
        encoded_message = message.encode('utf-8')

        # 2. Compress message
        compressed_message = zlib.compress(encoded_message)

        # 3. Create connection to address
        sock = socket.create_connection(address, timeout=10)

        # 4. Send message to address
        sock.send(encoded_message)  # TODO: Send compressed message instead

        # 5. decode/decompress response and return it
        response = sock.recv(1024)
        sock.close()
        if not response:
            return None
        else:
            return response.decode('utf-8')

    # Method to send/receive message to slot
    def _send_receive(self, message, slot, send_to_backup=False):
        address = self.nets_store.table[slot]
        try:
            result = self._send_receive_to_address(message, address)
            return result

        except socket.timeout:
            result = None
            if send_to_backup:
                # Get the backup
                backup = self.nets_store.get_backup(slot)
                if backup is not None:
                    # Send the message to the backup
                    result = self._send_receive(message, backup, send_to_backup=False)

            self.remove_host(address, local=False)

            return result

    # add host port [x]
    # Triggered when received "[exec] add address.host address.port"
    def add_host(self, address, respond):
        changes = self.nets_store.add(address)
        # resolve changes
        self._resolve_changes(changes)
        return respond('Host {0} added'.format(address))

    def _send_exec_add_host(self, address, remote_address):
        host, port = address
        message = "[exec] add {0} {1}".format(host, port)
        return self._send_receive_to_address(message, remote_address)

    def _send_exec_remove_host(self, address, remote_address):
        host, port = address
        message = "[exec] remove {0} {1}".format(host, port)
        return self._send_receive_to_address(message, remote_address)

    def remove_host(self, address, local=True):
        changes = self.nets_store.remove(address)
        self._resolve_changes(changes)

        if not local:
            remote_addresses = self.nets_store.get_remote_addresses()
            for remote_address in remote_addresses:
                self._send_exec_remove_host(address, remote_address)

        return 'Host {0} removed'.format(address)

    def _send_update_address(self, old_address, new_address, remote_address):
        message = "update {0} {1} {2} {3}".format(old_address[0], old_address[1], new_address[0], new_address[1])
        return self._send_receive_to_address(message, remote_address)

    def update_address(self, old_address, new_address, local=True):
        self.nets_store.update_address(old_address, new_address)
        if not local:
            # Broadcast store update to all remote addresses
            remote_addresses = self.nets_store.get_remote_addresses()
            for remote_address in remote_addresses:
                if not remote_address == new_address:
                    self._send_update_address(old_address, new_address, remote_address)

        return 'Address updated!'

    # Request to join
    # Triggered when received "add B.host B.port"
    def request_to_join(self, address):
        host, port = self.nets_store.local
        message = "[join] add {0} {1}".format(host, port)
        return self._send_receive_to_address(message, address)

    # resolve request to join from some server [ ]
    # Triggered when received "[join] add B.host B.port"
    def resolve_request_to_join(self, address, respond):
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

        # Respond with Acknowledgement
        respond('Joined Linda session successfully')

        print('Send table dump to new address')
        # 4. Dump new table on new address
        self._send_table_dump(address)

        print('Resolve changes')
        # 5. Resolve changes
        self._resolve_changes(changes)

        print('Resolved request to join')
        return None


    # send table [x]
    def _send_table_dump(self, address):
        command = '[dump] add {0}'.format(' '.join(['{0} {1}'.format(x[0], x[1]) for x in self.nets_store.table]))
        return self._send_receive_to_address(command, address)

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
        tuples = self.tuple_store.read_all(lambda t: hash_tuple(t) == slot)
        for t in tuples:
            self._insert_remote(t, address)

    def _insert_remote(self, t, address):
        message = "[exec] out{0}".format(t)
        return self._send_receive_to_address(message, address)

    def insert(self, t):
        slot = hash_tuple(t)
        address = self.nets_store.table[slot]

        if address == self.nets_store.local:
            return self.tuple_store.insert(t)
        else:
            return self._insert_remote(t, address)

    def _read_remote(self, description, address):
        t_str = _description_to_tuple_string(description)
        message = "[exec] rd{0}".format(t_str)
        return self._send_receive_to_address(message, address)

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
        t_str = _description_to_tuple_string(description)
        message = "[exec] in{0}".format(t_str)
        return self._send_receive_to_address(message, address)

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



