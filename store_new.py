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
# Dump table  [exec] add IP1 port1 IP2 port2 ...
import hashing

# # In-memory nets table for testing
# nets_store = []

class Store:
    def __init__(self):
        self.local = None
        self.table = []

    def create(self, address):
        self.local = address

    # def add_host(address):
    #     t0 = list(nets_store.table)
    #     nets_store.add(address)
    #     t1 = list(nets_store.table)
    #     d = hashing.diff(t0, t1)
    #     changes = hashing.get_changes_for_item(nets_store.local, d)
    #     return changes

    # Add an address and return the changes
    def add(self, address):
        if len(self.table) == 0:
            self.table = hashing.consistent_hash([self.local, address])
        self.table = hashing.add_item(address, self.table)

    def remove(self, address):
        self.table = hashing.remove_item(address, self.table)
        if hashing.get_num_items(self.table) == 1:
            self.table = []

    def set_local(self, address):
        def _set_address(x):
            if x == self.local:
                return address
            else:
                return x
        self.table = [_set_address(x) for x in self.table]
        self.local = address


nets_store = Store()


def create_store(address):
    nets_store.create(address)


def add_host(address):
    t0 = list(nets_store.table)
    nets_store.add(address)
    t1 = list(nets_store.table)
    d = hashing.diff(t0, t1)
    changes = hashing.get_changes_for_item(nets_store.local, d)
    return changes


def remove_host(address):
    nets_store.remove(address)


# # Create new table
# def create_store(address):
#     # Code to create new table
#     # 1. Create a random ID/name for this server process
#     # 2. Create the local table with given id name and host/port
#     global nets_store
#     table = hashing.consistent_hash([address])
#     print(table)
#     #print([address].extend(table))
#     #nets_store = [address].extend(table)
#     print([address])
#     nets_store = [address]
#     print(nets_store)
#     nets_store.extend(table)
#     print(nets_store)


# # Add a new host/port
# def add_host(address):
#     # Code to add a new host
#     # 1. Send request to given linda server host/port
#     # 2. Get address/slot table as response
#     # 3. Replace current table with given table
#     global nets_store
#     table = nets_store[1:]
#     new_table = hashing.add_item(address, table)
#     nets_store = [nets_store[0]].extend(new_table)
#
#
# # Remove a host/port
# def remove_host(address):
#     # Code to remove a host from the linda server
#     global nets_store
#     table = nets_store[1:]
#     new_table = hashing.add_item(address, table)
#     nets_store = [nets_store[0]].extend(new_table)

