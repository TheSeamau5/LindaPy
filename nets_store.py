import csv
import os

import hashing
import constants


class NetsStore:
    def __init__(self, session_name, address):
        self.session_name = session_name
        self.local = address
        self.table = hashing.consistent_hash([self.local, address])
        self._create_file()
        self.persist_to_disk()

    def _create_file(self):
        self.file_paths = constants.generate_file_paths(self.session_name)
        student_directory = self.file_paths['STUDENT_DIRECTORY']
        linda_directory = self.file_paths['LINDA_DIRECTORY']
        machine_directory = self.file_paths['MACHINE_DIRECTORY']
        nets_file_path = self.file_paths['NETS_FILE_PATH']

        if not os.path.exists(student_directory):
            os.makedirs(student_directory, mode=0o0777)

        if not os.path.exists(linda_directory):
            os.makedirs(linda_directory, mode=0o0777)

        if not os.path.exists(machine_directory):
            os.makedirs(machine_directory, mode=0o0777)

        if not os.path.exists(nets_file_path):
            open(nets_file_path, 'w+')
            os.chmod(nets_file_path, 0o0666)

    # Add an address and return the changes
    def add(self, address):
        old_table = list(self.table)
        self.table = hashing.add_item(address, self.table)
        new_table = list(self.table)
        diff = hashing.diff(old_table, new_table)
        changes = hashing.get_changes_for_item(self.local, diff)
        self.persist_to_disk()
        return changes

    # Remove an address and return the changes
    def remove(self, address):
        old_table = list(self.table)
        self.table = hashing.remove_item(address, self.table)
        new_table = list(self.table)
        diff = hashing.diff(old_table, new_table)
        changes = hashing.get_changes_for_item(self.local, diff)
        self.persist_to_disk()
        return changes

    # Get a list of all the addresses
    def get_addresses(self):
        return list(set(self.table))

    def update_local(self, address):
        def _set_address(x):
            if x == self.local:
                return address
            else:
                return x
        self.table = [_set_address(x) for x in self.table]
        self.local = address
        self.persist_to_disk()

    def persist_to_disk(self):
        nets_file_path = self.file_paths['NETS_FILE_PATH']
        with open(nets_file_path, 'w') as file:
            writer = csv.writer(file)
            # The local address always comes first
            writer.writerow(list(self.local))
            # Then we just write each address in the table
            for address in self.table:
                writer.writerow(list(address))
