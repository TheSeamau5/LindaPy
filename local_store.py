import ast
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

    # Write the in-memory store to the local store
    with open(NETS_FILE_PATH, 'w') as file:
        writer = csv.writer(file)
        for address in addresses:
            writer.writerow(list(address))

    # Done
    return True


# Remove a host from the local store
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

    # Write the in-memory store to the local store
    with open(NETS_FILE_PATH, 'w') as file:
        writer = csv.writer(file)
        for address in addresses:
            writer.writerow(list(address))

    # Done
    return True


# Put a tuple in the tuple store
def put_tuple(tupl):
    # In-memory tuple store
    tuples = []

    # Read all the tuples in memory
    with open(TUPLE_FILE_PATH, 'r') as file:
        for t in csv.reader(file, delimiter=','):
            tuples.append(t)

    # Add the new tuple to in-memory store
    tuples.append(tupl)

    # Write the in-memory store to the local store
    with open(TUPLE_FILE_PATH, 'w') as file:
        writer = csv.writer(file)
        for t in tuples:
            writer.writerow(list(t))

    # Done
    return tupl


def read_tuple(predicate):
    # In-memory tuple store
    tuples = []

    # Read all the tuples in memory
    with open(TUPLE_FILE_PATH, 'r') as file:
        for line in [l.rstrip('\n') for l in file]:
            tupl = ast.literal_eval(line)
            tuples.append(tupl)

    try:
        # Look for a tuple that matches the predicate
        result = next(t for t in tuples if predicate(t))
        return result
    except:
        # If a tuple was not found, return None
        return None


# Remove a tuple from the tuple store
def remove_tuple(predicate):
    # In-memory tuple store
    tuples = []

    # Read all the tuples in memory
    with open(TUPLE_FILE_PATH, 'r') as file:
        for line in [ l.rstrip('\n') for l in file ]:
            tupl = ast.literal_eval(line)
            tuples.append(tupl)

    try:
        # Look for a tuple that matches the predicate
        result = next(t for t in tuples if predicate(t))
    except:
        # If a tuple was not found, return None
        return None

    # A result was found, therefore do not include it back to the store
    tuples.remove(result)

    with open(TUPLE_FILE_PATH, 'w') as file:
        for t in tuples:
            file.write('{0}\n'.format(t))

    return result
