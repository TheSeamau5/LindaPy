import select
import socket
import threading
import uuid

from queue import Queue, Empty


from store import Store
from interpreter import *


class Server:
    # state:
    #   - socket
    #   - local_address
    #   - read_sockets
    #   - write_sockets
    #   - message_queues
    def __init__(self, session_name=uuid.uuid4().hex):
        # Set session name
        self.session_name = session_name
        print('Starting new session with name: {0}'.format(self.session_name))

        # Get the local IP address by making a bogus socket connection
        dgram_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        dgram_socket.connect(('8.8.8.8', 53))
        host = dgram_socket.getsockname()[0]

        # Create the socket server
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Bind the server to some available port
        self.socket.bind(('', 0))
        port = self.socket.getsockname()[1]

        self.local_address = (host, port)

        # Create store
        store = Store(self.session_name, self.local_address)

        # Set interpreter
        self.interpreter = Interpreter(store)

        print('Starting server on {0} port: {1}'.format(host, port))

        # Listen for incoming connections
        self.socket.listen(5)

        # Sockets from which we expect to read
        self.read_sockets = [self.socket]

        # Sockets to which we expect to write
        self.write_sockets = []

        # Outgoing message queues for each socket
        # message_queues : { Socket : Queue }
        self.message_queues = {}

        self.run()

    def _remove_socket(self, s):
        # Stop listening for input from the client
        if s in self.read_sockets:
            self.read_sockets.remove(s)

        # Remove the socket from the output sockets
        if s in self.write_sockets:
            self.write_sockets.remove(s)

        # Close the socket
        s.close()

        del self.message_queues[s]

    def _process_command(self, command, client):
        print('Processing command: {0}'.format(command))
        response = str(self.interpreter.interpret(command))
        client.send(response.encode('utf-8'))

    def _process_message(self, message, client):
        thread = threading.Thread(target=self._process_command, args=(message, client))
        thread.start()

    def run(self):
        while self.read_sockets:
            try:
                print('Waiting for input')

                readable, writable, exceptional = select.select(self.read_sockets, self.write_sockets, self.read_sockets)

                # Handle read sockets
                for s in readable:

                    # If the read socket is the current server
                    # We add the client to the list of read sockets
                    # This way we can respond to it
                    if s is self.socket:
                        client, (client_host, client_port) = s.accept()

                        print('New connection from {0} port: {1}'.format(client_host, client_port))

                        # Make the connection to the client non-blocking
                        client.setblocking(0)

                        self.read_sockets.append(client)

                        # Start a new message queue for the new client
                        # This message queue will allow us to later respond
                        # To the new client
                        self.message_queues[client] = Queue()

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
                            self.message_queues[s].put(message)

                            # Add output channel for response
                            if s not in self.write_sockets:
                                self.write_sockets.append(s)

                        else:
                            # Interpret empty result as closed connection
                            print('Closing connection with {0} port: {1}'.format(client_host, client_port))

                            self._remove_socket(s)


                # Handle write sockets
                for s in writable:
                    print('Handling write sockets')

                    client_host, client_port = s.getpeername()

                    try:
                        # Get the next message in the queue
                        print('Get the next message in the queue')
                        message = self.message_queues[s].get_nowait()

                        # Interpreting message
                        self._process_command(message, s)

                    except Empty:
                        # Message queue is empty
                        print('Message queue for {0} port: {1} is empty'.format(client_host, client_port))
                        self.write_sockets.remove(s)

                # Handle "exceptional conditions:
                # i.e. something went wrong
                for s in exceptional:
                    client_host, client_port = s.getpeername()
                    print('Handling exceptional condition for {0} port: {1}'.format(client_host, client_port))
                    self._remove_socket(s)

            except:
                pass
