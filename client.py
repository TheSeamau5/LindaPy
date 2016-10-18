import socket

import local_store

from parser import parse

host, port = local_store.get_local_address()

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

client.connect((host, int(port)))


while True:
    command = input('linda> ')

    if parse(command):
        client.send(command.encode('utf-8'))
        print('Sending {0} to {1} port: {2}'.format(command, host, port))

        data = client.recv(1024)
        print('Received Data: {0} '.format(data))

        if not data:
            client.close()
        else:
            result = data.decode('utf-8')
            print(result)