import ast
import os

import constants


class TupleStore:
    def __init__(self, session_name):
        self.session_name = session_name
        self.tuples = []
        self._create_file()

    def _create_file(self):
        self.file_paths = constants.generate_file_paths(self.session_name)
        student_directory = self.file_paths['STUDENT_DIRECTORY']
        linda_directory = self.file_paths['LINDA_DIRECTORY']
        machine_directory = self.file_paths['MACHINE_DIRECTORY']
        tuples_file_path = self.file_paths['TUPLES_FILE_PATH']

        if not os.path.exists(student_directory):
            os.makedirs(student_directory, mode=0o0777)

        if not os.path.exists(linda_directory):
            os.makedirs(linda_directory, mode=0o0777)

        if not os.path.exists(machine_directory):
            os.makedirs(machine_directory, mode=0o0777)

        if not os.path.exists(tuples_file_path):
            open(tuples_file_path, 'w+')
            os.chmod(tuples_file_path, 0o0666)
        else:
            self.load_from_disk()

    def insert(self, t):
        self.tuples.append(t)
        self.persist_to_disk()
        return t

    def read(self, predicate):
        try:
            result = next(t for t in self.tuples if predicate(t))
            return result
        except:
            return None

    def remove(self, predicate):
        try:
            result = next(t for t in self.tuples if predicate(t))
            self.tuples.remove(result)
            self.persist_to_disk()
            return result
        except:
            return None

    def read_all(self, predicate):
        return [x for x in self.tuples if predicate(x)]

    def remove_all(self, predicate):
        result = [x for x in self.tuples if predicate(x)]
        self.tuples = [x for x in self.tuples if not predicate(x)]
        self.persist_to_disk()
        return result

    def persist_to_disk(self):
        tuples_file_path = self.file_paths['TUPLES_FILE_PATH']
        with open(tuples_file_path, 'w') as file:
            for t in self.tuples:
                file.write('{0}\n'.format(t))

    def load_from_disk(self):
        tuples_file_path = self.file_paths['TUPLES_FILE_PATH']
        tuples = []

        with open(tuples_file_path, 'r') as file:
            for line in [l.rstrip('\n') for l in file]:
                t = ast.literal_eval(line)
                tuples.append(t)

        self.tuples = tuples
