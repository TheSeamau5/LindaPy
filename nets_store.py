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


class NetsStore:
    def __init__(self, address):
        self.local = address
        self.table = hashing.consistent_hash([self.local, address])

    # Add an address and return the changes
    def add(self, address):
        old_table = list(self.table)
        self.table = hashing.add_item(address, self.table)
        new_table = list(self.table)
        diff = hashing.diff(old_table, new_table)
        changes = hashing.get_changes_for_item(self.local, diff)
        return changes

    # Remove an address and return the changes
    def remove(self, address):
        old_table = list(self.table)
        self.table = hashing.remove_item(address, self.table)
        new_table = list(self.table)
        diff = hashing.diff(old_table, new_table)
        changes = hashing.get_changes_for_item(self.local, diff)
        return changes

    def update_local(self, address):
        def _set_address(x):
            if x == self.local:
                return address
            else:
                return x
        self.table = [_set_address(x) for x in self.table]
        self.local = address
