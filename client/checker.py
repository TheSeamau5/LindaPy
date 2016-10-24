from parser import parse

class Checker:
    def check(self, command):
        try:
            tree = parse(command)

            if 'directive' in tree:
                print('Syntax Error')
                return False

            if  tree['command'] not in ['add', 'remove', 'out', 'rd', 'in']:
                print('Command "{0}" not supported'.format(tree['command']))
                return False

            if tree['command'] == 'out':
                for arg in tree['args']:
                    if not arg['type'] == 'value':
                        print('"out" does not accept variables')
                        return False

            if tree['command'] == 'rd' or tree['command'] == 'in':
                for arg in tree['args']:
                    if arg['type'] == 'variable' and arg['kind'] not in ['int', 'str', 'float']:
                        print('Only variables of type "int", "str", or "float" can be queried')
                        return False

            return True

        except:
            print('Syntax Error')
            return False
