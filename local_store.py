import csv
import os

from constants import *

# Create local files and store for Linda
def create_local_store():
    # Create Student directory
    if not os.path.exists(STUDENT_DIRECTORY):
        os.makedirs(STUDENT_DIRECTORY, mode=0o0777)

    # Create Linda directory
    if not os.path.exists(LINDA_DIRECTORY):
        os.makedirs(LINDA_DIRECTORY, mode=0o0777)

    # Create Nets File
    if not os.path.exists(NETS_FILE_PATH):
        open(NETS_FILE_PATH, 'w+')

    # Create Tuple File
    if not os.path.exists(TUPLE_FILE_PATH):
        open(TUPLE_FILE_PATH, 'w+')


# Linda API:
# add host
# remove host
# put tuple
# read tuple
# remove tuple

# Add a host to the local store
def add_host(host, port):
    # In-memory address store
    addresses = []

    # Read all the addresses in memory
    with open(NETS_FILE_PATH, 'r') as file:
        for address in csv.reader(file, delimiter=','):
            addresses.append(address)

    # Add the new address to in-memory store
    addresses.append((host, port))

    # Write the in-memory story to the local store
    with open(NETS_FILE_PATH, 'w') as file:
        writer = csv.writer(file)
        for address in addresses:
            writer.writerow(list(address))

    # Done
    return True


def remove_host(host, port):
    # In-memory address store
    addresses = []

    # Read all the addresses in memory
    with open(NETS_FILE_PATH, 'r') as file:
        for address in csv.reader(file, delimiter=','):
            current_host, current_port = address

            # Only read addresses that are not the current host and port
            if not (host == current_host and port == current_port):
                addresses.append(address)

    # Write the in-memory story to the local store
    with open(NETS_FILE_PATH, 'w') as file:
        writer = csv.writer(file)
        for address in addresses:
            writer.writerow(list(address))

    # Done
    return True

