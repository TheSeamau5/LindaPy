import csv
import socket
import zlib

import constants
from checker import Checker


class Client:
    def __init__(self, session_name):
        self.session_name = session_name

        # Create the command checker
        self.checker = Checker()

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

            # Check the command first before sending it
            if self.checker.check(command):
                # Send the command to the server
                encoded_command = command.encode('utf-8')
                compressed_command = zlib.compress(encoded_command)
                self.socket.send(compressed_command)

                # Get response from server
                data = self.socket.recv(1024)

                if not data:
                    # Consider no response as end of socket connection
                    self.socket.close()
                else:
                    # Decode the result and print it
                    uncompressed_data = zlib.decompress(data)
                    result = uncompressed_data.decode('utf-8')
                    print(result)
