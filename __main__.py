import sys

from server import *

if len(sys.argv) > 1:
    session_name = sys.argv[1]
    Server(session_name)
else:
    Server()