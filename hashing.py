# Consistent Hashing Implementation
import hashlib

# Function to generate a hash table generator
# n : number of slots (default = 1024)
def generate_consistent_hasher(n=1024):
    # Generate a table
    def generate_table(addresses):
        m = len(addresses)
        r = n / m
        return [addresses[int(i // r)] for i in range(n)]

    return generate_table

n = 8
#n = 1024
# Consistent hash function
consistent_hash = generate_consistent_hasher(n)


def hash_tuple(t):
    m = hashlib.md5()
    m.update(str(t).encode('utf-8'))
    return int(m.hexdigest(), 16) % n


# Find the indices that have changed between two lists
# Assuming l1 and l2 have the same length
def diff_indices(l1, l2):
    return [i for i in range(min(len(l1), len(l2))) if not l1[i] == l2[i]]


# Get the number of different items in a table
def get_num_items(table):
    return len(set(table))


# Convert a table to a dictionary
def convert_to_dict(table):
    d = {}
    for i in range(len(table)):
        item = table[i]
        if item in d:
            d[item].append(i)
        else:
            d[item] = [i]
    return d


# Convert a dictionary to a table
def convert_to_list(d):
    l = []
    for item, indices in d.items():
        for index in indices:
            l.append((index, item))
    sorted_l = sorted(l, key = lambda item: item[0])
    return [x[1] for x in sorted_l]


def _get_indices_for_new_item(table):
    d = convert_to_dict(table)
    num_items = get_num_items(table)
    total = len(table)
    n = round(total / (num_items + 1))
    l = [(k, v) for k, v in d.items()]
    sorted_l = sorted(l, key= lambda item: len(item[1]), reverse=True)
    indices = []
    num_items = len(l)
    for i in range(n):
        item_index = i % num_items
        inner_index = i // num_items
        k, v = sorted_l[item_index]
        if inner_index < len(v):
            index = v[inner_index]
            indices.append(index)
    return indices


def split_into_chunks(l, k):
    n = len(l)
    if n == 0 or k <= 1:
        return [l]

    chunks = []
    current_chunk = []
    current_chunk_index = 0
    for i in range(n):
        f = i / n
        j = current_chunk_index + 1
        g = j / k
        if f >= g:
            current_chunk_index += 1
            chunks.append(current_chunk)
            current_chunk = []
        current_chunk.append(l[i])

    if len(current_chunk):
        chunks.append(current_chunk)

    return chunks


def _get_index_dict_for_remove_item(item, d):
    # print('Dict:')
    # print(d)
    # print('Item: {0}'.format(item))
    result = {}
    sorted_kv_list = sorted([(k, v) for k, v in d.items() if not k == item], key=lambda kv: len(kv[1]))
    keys = [k for k, v in sorted_kv_list if not k == item]
    # print('Keys: {0}'.format(keys))
    num_keys = len(keys)
    indices = d[item]
    num_chunks = max(1, min(num_keys, len(indices)))
    chunks = split_into_chunks(indices, num_chunks)
    # print('Chunks:')
    # print(chunks)
    for i in range(len(chunks)):
        # print('i: '.format(i))
        # print('Key i: {0}'.format(keys[i]))
        # print('Chunk i: {0}'.format(chunks[i]))
        result[keys[i]] = chunks[i]

    return result


def generate_replica_table(table):
    descriptive_table = [
        {
            'item': table[i],
            'index': i,
            'replica': None,
            'available': True

        } for i in range(len(table))
    ]

    n = len(table)
    for i in range(n):
        x = descriptive_table[i]
        for j in range(n):
            k = (i + j) % n
            y = descriptive_table[k]
            if not (x['item'] == y['item']) and y['available'] is True:
                x['replica'] = y['index']
                y['available'] = False
                break

    def _get_replica(x):
        if x['replica'] is None:
            return (x['index'], x['item']), (None, None)
        else:
            return (x['index'], x['item']), (x['replica'], table[x['replica']])

    return [_get_replica(x) for x in descriptive_table]


def remove_item(item, table):
    new_table = table
    if item in table:
        d = convert_to_dict(table)
        index_dict = _get_index_dict_for_remove_item(item, d)
        del d[item]
        for item, indices in index_dict.items():
            if item in d:
                d[item].extend(indices)
            else:
                d[item] = [indices]
        new_table = convert_to_list(d)
    return new_table


def add_item(item, table):
    new_table = list(table)
    if item not in table:
        new_indices = _get_indices_for_new_item(table)
        for i in new_indices:
            new_table[i] = item
    return new_table


def diff(table1, table2):
    diffs = []
    n = len(table1)
    if n == len(table2):
        replica_table1 = generate_replica_table(table1)
        replica_table2 = generate_replica_table(table2)

        for i in range(n):
            ((source_index1, source_value1), (backup_index1, backup_value1)) = replica_table1[i]
            ((source_index2, source_value2), (backup_index2, backup_value2)) = replica_table2[i]

            if i == source_index1 == source_index2:
                # We consider there to be a change if either:
                #       - source_value has changed
                #       - backup_value has changed
                if (not source_value1 == source_value2) or (not backup_value1 == backup_value2):
                    diffs.append((replica_table1[i], replica_table2[i]))

    return diffs


# Find which data to send to where given an item
def get_changes_for_item(item, diffs):
    changes = []

    for diff in diffs:
        replica1, replica2 = diff

        ((source_index1, source_value1), (backup_index1, backup_value1)) = replica1
        ((source_index2, source_value2), (backup_index2, backup_value2)) = replica2

        if source_value1 == item:
            if not source_value2 == item:
                changes.append((source_index1, source_value2))
                continue

        if backup_value1 == item:
            if not backup_value2 == item:
                changes.append((source_index1, backup_value2))
                continue

    return list(set(changes))
