import sys

from client import Client

try:
    session_name = sys.argv[1]
    Client(session_name)
except:
    print('Please enter the server\'s subdirectory name')