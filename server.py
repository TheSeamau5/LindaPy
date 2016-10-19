import select
import socket
import threading

from queue import Queue, Empty

import interpreter
import local_store


def run():
    # Get the local IP address by making a bogus socket connection
    dgram_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    dgram_socket.connect(('8.8.8.8', 53))
    host = dgram_socket.getsockname()[0]

    # Create the local store
    local_store.create_local_store()


    # Create the server
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Bind the server to some available port
    server.bind(('', 0))
    port = server.getsockname()[1]

    print('Starting server on {0} port: {1}'.format(host, port))

    # Store the local address
    local_store.set_local_host(host, port)

    # Listen for incoming connections
    server.listen(5)

    # Sockets from which we expect to read
    read_sockets = [server]

    # Sockets to which we expect to write
    write_sockets = []

    # Outgoing message queues for each socket
    # message_queues : { Socket : Queue }
    message_queues = {}


    def remove_socket(s):
        # Stop listening for input from the client
        if s in read_sockets:
            read_sockets.remove(s)

        # Remove the socket from the output sockets
        if s in write_sockets:
            write_sockets.remove(s)

        # Close the socket
        s.close()

        # Remove the message queue associated to this socket
        del message_queues[s]


    def process_command(command, client):
        print('Processing command: {0}'.format(command))
        response = str(interpreter.interpret(command))
        print('Response: {0}'.format(response))
        client.send(response.encode('utf-8'))


    def process_message(message, client):
        thread = threading.Thread(target = process_command, args = (message, client))
        thread.start()


    # Server loop
    # The server will always run here and never crash
    while read_sockets:
        try:
            print('Waiting for input')

            readable, writable, exceptional = select.select(read_sockets, write_sockets, read_sockets)

            # Handle read sockets
            for s in readable:

                # If the read socket is the current server
                # We add the client to the list of read sockets
                # This way we can respond to it
                if s is server:
                    client, (client_host, client_port) = s.accept()

                    print('New connection from {0} port: {1}'.format(client_host, client_port))

                    # Make the connection to the client non-blocking
                    client.setblocking(0)

                    read_sockets.append(client)

                    # Start a new message queue for the new client
                    # This message queue will allow us to later respond
                    # To the new client
                    message_queues[client] = Queue()

                # If the read socket is not the current server
                # We simply receive data from the client and
                # Put the message on the message queue
                else:
                    # Receive data from the client
                    data = s.recv(1024)

                    client_host, client_port = s.getpeername()

                    if data:
                        # Decode data to string
                        message = data.decode('utf-8')

                        print('Received "{0}" from {1} port: {2}'.format(message, client_host, client_port))

                        # Put the message on the message queue
                        message_queues[s].put(message)

                        # Add output channel for response
                        if s not in write_sockets:
                            write_sockets.append(s)

                    else:
                        # Interpret empty result as closed connection
                        print('Closing connection with {0} port: {1}'.format(client_host, client_port))

                        remove_socket(s)

            # Handle write sockets
            for s in writable:
                print('Handling write sockets')

                client_host, client_port = s.getpeername()

                try:
                    # Get the next message in the queue
                    print('Get the next message in the queue')
                    message = message_queues[s].get_nowait()

                    # Interpreting message
                    process_message(message, s)

                except Empty:
                    # Message queue is empty
                    print('Message queue for {0} port: {1} is empty'.format(client_host, client_port))
                    write_sockets.remove(s)

            # Handle "exceptional conditions:
            # i.e. something went wrong
            for s in exceptional:
                client_host, client_port = s.getpeername()

                print('Handling exceptional condition for {0} port: {1}'.format(client_host, client_port))

                remove_socket(s)

        except:
            pass

