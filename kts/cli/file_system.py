import os


def check_structure(paths):
    for path in paths:
        if not os.path.isdir(path):
            return False
    return True


IMPORT_ERROR_MESSAGE = """
This directory doesn't look like kts project.
Use `kts init` to initialize a project. You can't use kts without its file system.
This error could also be raised by importing kts from a directory of existing project other than /notebooks.
"""


def check_file_system():
    paths = ['../input', '../notebooks', '../storage/info', '../storage/sources', '../output']

    if check_structure(paths):
        return

    raise ImportError(IMPORT_ERROR_MESSAGE)
