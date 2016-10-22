import csv
import socket
import sys

import constants


class Client:
    def __init__(self, session_name):
        self.session_name = session_name

        # Create the socket server
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Get the server address
        self.server_address = self._get_server_address()

        # Run the server
        self.run()

    def _get_server_address(self):
        file_paths = constants.generate_file_paths(self.session_name)
        nets_file_path = file_paths['NETS_FILE_PATH']
        with open(nets_file_path, 'r') as file:
            for (host, port) in csv.reader(file, delimiter=','):
                return host, int(port)

    def run(self):
        # Connect to server
        self.socket.connect(self.server_address)

        # Loop
        while True:
            # Get the command from user input
            command = input('linda> ')

            # Send the command to the server
            self.socket.send(command.encode('utf-8'))

            # Get response from server
            data = self.socket.recv(1024)

            if not data:
                # Consider no response as end of socket connection
                self.socket.close()
            else:
                # Decode the result and print it
                result = data.decode('utf-8')
                print(result)


# For testing
try:
    session_name = sys.argv[1]
    Client(session_name)
except Exception as e:
    print(e)