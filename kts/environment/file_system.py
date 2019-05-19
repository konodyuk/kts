import os
import json
from .. import config
from warnings import warn


def find_root_dir():
    i = 0
    while os.path.realpath('../' * i) != '/':
        if os.path.exists('../' * i + '.kts'):
            if i != 0:
                return '../' * i
            else:
                return './'
        i += 1
    return False


def parse(file):
    with open(file) as f:
        return json.load(f)


def get_mode():
    if find_root_dir():
        config.storage_path = parse(find_root_dir() + '.kts')['storage_path']
        config.mode = 'local'
        config.root_dir = find_root_dir()
    else:
        warn("Couldn't find existing kts project. Setting kaggle-mode")
        config.mode = 'kaggle'
        config.root_dir = '.'


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
