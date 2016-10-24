STUDENT_ID = '90849'
STUDENT_DIRECTORY = '/tmp/{0}'.format(STUDENT_ID)


def generate_file_paths(name):
    machine_directory = '{0}/{1}'.format(STUDENT_DIRECTORY, name)
    linda_directory = '{0}/linda'.format(machine_directory)
    nets_file_path = '{0}/nets'.format(linda_directory)
    tuples_file_path = '{0}/tuples'.format(linda_directory)
    return {
        'STUDENT_DIRECTORY': STUDENT_DIRECTORY,
        'MACHINE_DIRECTORY': machine_directory,
        'LINDA_DIRECTORY': linda_directory,
        'NETS_FILE_PATH': nets_file_path,
        'TUPLES_FILE_PATH': tuples_file_path
    }
