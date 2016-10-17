import parser
import store


def evaluate(tree):
    if tree['command'] == 'add':
        host = tree['args']['host']
        port = tree['args']['port']
        return store.add_host(host, port)

    elif tree['command'] == 'remove':
        host = tree['args']['host']
        port = tree['args']['port']
        return store.remove_host(host, port)

    elif tree['command'] == 'out':
        # Convert description to tuple
        tupl = []
        for item in tree['args']:
            if item['type'] == 'value':
                tupl.append(item['value'])
            else:
                print('"out" only accepts value tuples')
                return None

        tupl = tuple(tupl)

        if tree['directive'] and tree['directive'] == 'exec':
            return store.put_tuple_locally(tupl)
        else:
            return store.put_tuple(tupl)

    elif tree['command'] == 'rd':
        description = tree['args']

        if tree['directive'] and tree['directive'] == 'exec':
            return store.read_tuple_locally(description)
        else:
            return store.read_tuple(description)

    elif tree['command'] == 'in':
        description = tree['args']

        if tree['directive'] and tree['directive'] == 'exec':
            return store.remove_tuple_locally(description)
        else:
            return store.remove_tuple(description)

    else:
        print('Command "{0}" unknown'.format(tree['command']))
        return None


def interpret(command):
    try:
        tree = parser.parse(command)
        return evaluate(tree)

    except:
        return None