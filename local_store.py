import os

from constants import *

# Create local files and store for Linda
def create_local_store():
    # Create Student directory
    if not os.path.exists(STUDENT_DIRECTORY):
        os.makedirs(STUDENT_DIRECTORY, mode=0o0777)

    # Create Linda directory
    if not os.path.exists(LINDA_DIRECTORY):
        os.makedirs(LINDA_DIRECTORY, mode=0o0777)

    # Create Nets File
    if not os.path.exists(NETS_FILE_PATH):
        open(NETS_FILE_PATH, 'w+')

    # Create Tuple File
    if not os.path.exists(TUPLE_FILE_PATH):
        open(TUPLE_FILE_PATH, 'w+')


