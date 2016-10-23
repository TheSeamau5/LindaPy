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
# Dump table  [dump] add IP1 port1 IP2 port2 ...
from parser import parse


class Interpreter:
    def __init__(self, store):
        self.store = store

    # respond: a -> ()
    def evaluate(self, tree, respond):
        if tree['command'] == 'add':
            # [join] B.host B.port
            if 'directive' in tree and tree['directive'] == 'join':
                print('This is a join add')
                for arg in tree['args']:
                    address = (arg['host'], arg['port'])
                    self.store.resolve_request_to_join(address, respond)
                    return None

            elif 'directive' in tree and tree['directive'] == 'exec':
                for arg in tree['args']:
                    address = (arg['host'], arg['port'])
                    self.store.add_host(address, respond)
                    return respond(None)

            elif 'directive' in tree and tree['directive'] == 'dump':
                print('This is a dump add')
                table = []
                for arg in tree['args']:
                    address = (arg['host'], arg['port'])
                    table.append(address)
                self.store.dump_table(table)
                return respond(None)

            # Normal case:
            else:
                print('This is a regular add')
                for arg in tree['args']:
                    address = (arg['host'], arg['port'])
                    self.store.request_to_join(address)
                    return respond(None)


        # elif tree['command'] == 'remove':
        #     for arg in tree['args']:
        #         address = (arg['host'], arg['port'])
        #         return self.nets_store.remove(address)

        if tree['command'] == 'out':
            t = []

            for item in tree['args']:
                if item['type'] == 'value':
                    t.append(item['value'])
                else:
                    print('"out" only accepts value tuples')
                    respond(None)
                    return None

            t = tuple(t)

            return respond(self.store.insert(t))

        elif tree['command'] == 'rd':
            description = tree['args']

            return respond(self.store.read(description, local=True))

        elif tree['command'] == 'in':
            description = tree['args']

            return respond(self.store.remove(description, local=True))

    # respond : a -> ()
    def interpret(self, command, respond):
        print('Interpreting command: {0}'.format(command))
        try:
            tree = parse(command)
            print('Tree: {0}'.format(tree))
            return self.evaluate(tree, respond)

        except Exception as e:
            print('Error: {0}'.format(e))
            return None
