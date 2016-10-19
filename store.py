import hashlib
import socket

from operator import itemgetter

import local_store

# The store is an abstraction that allows us to store
# tuples either locally or remotely

# The store uses this socket to send/receive remote commands
# store_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


# Function to hash a tuple into a number
def hash_tuple(tupl):
    m = hashlib.md5()
    m.update(str(tupl).encode('utf-8'))
    return int(m.hexdigest(), 16)



# Add a host
def add_host(host, port):
    # Adding host simply adds a host to the local store
    return local_store.add_host(host, port)


# Remove a host
def remove_host(host, port):
    # Removing host simply removes a host from the local store
    return local_store.remove_host(host, port)


# Put a tuple locally
def put_tuple_locally(tupl):
    return local_store.put_tuple(tupl)


# Put a tuple remotely
def put_tuple_remotely(tupl, address):
    # store_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # store_socket.setblocking(0)
    print('Connecting store socket to address: {0}'.format(address))
    # # First connect the store socket to the address
    # store_socket.connect(address)
    store_socket = socket.create_connection(address)
    # Send the message
    message = "[exec] out{0}".format(tupl)
    print('Sending Message: {0}'.format(message))
    store_socket.send(message.encode('utf-8'))
    print('Message sent and waiting for response')
    # Get a response back
    response = store_socket.recv(1024)
    print('Received response: {0}'.format(response))
    store_socket.close()
    # Return the response
    if not response:
        return None
    else:
        return response.decode('utf-8')


# Put a tuple
def put_tuple(tupl):
    print('Putting tuple: {0}'.format(tupl))
    addresses = sorted(local_store.get_all_addresses(), key = itemgetter(0, 1))
    print('Addresses: {0}'.format(addresses))
    local_address = local_store.get_local_address()
    print('Local Address: {0}'.format(local_address))

    index = hash_tuple(tuple) % len(addresses)
    address = addresses[index]
    print('Get address to send to: {0}'.format(address))

    if address == local_address:
        print('Putting tuple locally')
        # If address chosen is local address, store locally
        return put_tuple_locally(tupl)
    else:
        print('Putting tuple remotely')
        # Send remotely
        return put_tuple_remotely(tupl, address)


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


# Compare a tuple to a description
def match_description_to_tuple(description, tupl):
    print('Matching description to tuple')
    print('Description: {0}'.format(description))
    print('Description Length: {0}'.format(len(description)))
    print('Tuple: {0}'.format(tupl))
    print('Tuple Length: {0}'.format(len(tupl)))
    if len(description) == len(tupl) == 0:
        print('Description is empty')
        return True

    if len(description) == len(tupl):
        for i in range(len(description)):
            item = description[i]
            tuple_item = tupl[i]
            print('Description Item: {0}'.format(item))
            print('Tuple Item: {0}'.format(tuple_item))

            if item['type'] == 'value':
                print('item[\'value\'] = {0}'.format(item['value']))
                print('tuple_item = {0}'.format(tuple_item))
                if not (item['value'] == tuple_item):
                    return False

            elif item['type'] == 'variable':
                tuple_item_kind = tuple_item.__class__.__name__
                if not (item['kind'] == tuple_item_kind):
                    return False

            else:
                return False

        return True

    else:
        return False

# import pytest
# from pytest import list_of
#
#
# @pytest.mark.randomize(l=list_of(int), ncalls=1000)
# def test_match_description_to_tuple(l):
#     t = tuple(l)
#     description = [{'type': 'value', 'value': x} for x in l]
#     assert(match_description_to_tuple(description, t))
#
#
# @pytest.mark.randomize(l=list_of(int), ncalls=1000)
# def test_match_description_to_tuple_variable(l):
#     t = tuple(l)
#     description = [{'type': 'variable', 'kind': x.__class__.__name__} for x in l]
#     assert(match_description_to_tuple(description, t))



# Read a tuple remotely
def read_tuple_remotely(description, address):
    # store_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # store_socket.setblocking(0)
    # # First connect the store socket to the address
    # store_socket.connect(address)
    print('Creating store socket')
    store_socket = socket.create_connection(address)
    print('Store socket created')
    # Send the message
    message = "[exec] rd{0}".format(description_to_tuple_string(description))
    print('Message to be sent: {0}'.format(message))
    store_socket.send(message.encode('utf-8'))
    print('Message sent and waiting for response')
    # Get a response back
    response = store_socket.recv(1024)
    print('Received response: {0}'.format(response))
    # Return the response
    store_socket.close()
    print('Closed Store Socket')
    if not response:
        print('No response')
        return None
    else:
        return response.decode('utf-8')


# Read a tuple locally
def read_tuple_locally(description):
    return local_store.read_tuple(lambda t: match_description_to_tuple(description, t))


# Read a tuple
def read_tuple(description):
    # First try locally
    t = read_tuple_locally(description)
    print('Tuple Response: {0}'.format(t))
    print('Tuple Type: {0}'.format(t.__class__.__name__))
    # If none, loop through all the addresses and try remotely
    if t is not None:
        return t

    else:
        print('Reading Tuples Remotely')
        # Get all the addresses, omitting the local one (which is the first one)
        addresses = local_store.get_all_addresses()[1:]
        print('Addresses: {0}'.format(addresses))

        for address in addresses:
            print('Current Address we\'re querying: {0}'.format(address))
            t = read_tuple_remotely(description, address)
            print('Tuple Response: {0}'.format(t))
            print('Tuple Type: {0}'.format(t.__class__.__name__))

            if t is not None:
                return t

        return None


# Remove a tuple remotely
def remove_tuple_remotely(description, address):
    # store_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # store_socket.setblocking(0)
    # # First connect the store socket to the address
    # store_socket.connect(address)
    store_socket = socket.create_connection(address)
    # Send the message
    message = "[exec] in{0}".format(description_to_tuple_string(description))
    store_socket.send(message.encode('utf-8'))
    # Get a response back
    response = store_socket.recv(1024)
    # Return the response
    store_socket.close()
    if not response:
        return None
    else:
        return response.decode('utf-8')


# Remove a tuple locally
def remove_tuple_locally(description):
    return local_store.remove_tuple(lambda t: match_description_to_tuple(description, t))


# Remove a tuple
def remove_tuple(description):
    # First try locally
    t = remove_tuple_locally(description)
    # If none, loop through all the addresses and try remotely
    if not t is None:
        return t

    else:
        # Get all the addresses, omitting the local one (which is the first one)
        addresses = local_store.get_all_addresses()[1:]

        for address in addresses:
            t = remove_tuple_remotely(description, address)

            if t:
                return t

        return None