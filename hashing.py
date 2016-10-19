def generate_hasher(n = 1024):
    # Generate a table
    def generate_table(addresses):
        m = len(addresses)
        r = n / m
        return [addresses[int(i // r)] for i in range(n)]

    return generate_table


# Assuming l1 and l2 have the same length
def diff_indices(l1, l2):
    return [i for i in range(len(l1)) if not l1[i] == l2[i]]


def split_regions(table):
    result = []
    temp = []
    prev = None
    for item in table:
        if prev is None or prev == item:
            temp.append(item)

        else:
            result.append(temp)
            temp = [item]

        prev = item

    result.append(temp)
    return result


def _get_num_items(table):
    return len(set(table))


def convert_to_dict(table):
    d = {}
    for i in range(len(table)):
        item = table[i]
        if item in d:
            d[item].append(i)
        else:
            d[item] = [i]
    return d

# import pytest
# from pytest import list_of
#
# @pytest.mark.randomize(table=list_of(int), ncalls=1000)
# def test_convert_table(table):
#     d = convert_to_dict(table)
#     t = convert_to_list(d)
#     assert(t == table)


def convert_to_list(d):
    l = []
    for item, indices in d.items():
        for index in indices:
            l.append((index, item))
    sorted_l = sorted(l, key = lambda item: item[0])
    return [x[1] for x in sorted_l]


# def pick_indices_from_dict(ratio, d):
#     new_indices = []
#     for item, indices in d.items():
#         threshold = round((1 - ratio) * len(indices))
#         new_indices.extend(indices[threshold:])
#     return new_indices


def get_indices_for_new_item(table):
    d = convert_to_dict(table)
    num_items = _get_num_items(table)
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


def _generate_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def split_into_chunks(l, n):
    return list(_generate_chunks(l, n))


def get_index_dict_for_remove_item(item, d):
    print('Dict: {0}'.format(d))
    result = {}
    keys = [x for x in list(d.keys()) if not x == item]
    print('Keys: {0}'.format(keys))
    num_keys = len(keys)
    indices = d[item]
    chunks = split_into_chunks(indices, num_keys)
    print('Chunks: {0}'.format(chunks))
    for i in range(len(chunks)):
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

    return [(x['index'], x['replica']) for x in descriptive_table]


def remove_item(item, table):
    new_table = table
    if item in table:
        d = convert_to_dict(table)
        index_dict = get_index_dict_for_remove_item(item, d)
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
    if not item in table:
        new_indices = get_indices_for_new_item(table)
        for i in new_indices:
            new_table[i] = item
    return new_table

