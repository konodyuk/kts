import json
import os


def find_root_dir():
    """ """
    i = 0
    while os.path.realpath("../" * i) != "/":
        if os.path.exists("../" * i + "kts_config.py"):
            if i != 0:
                return "../" * i
            else:
                return "./"
        i += 1
    return False


def parse(file):
    """

    Args:
      file: 

    Returns:

    """
    with open(file) as f:
        return json.load(f)


def get_mode():
    """ """
    if find_root_dir():
        # config.STORAGE_PATH = parse(find_root_dir() + '.kts')['STORAGE_PATH']
        cache_mode = "disk_and_ram"
        cache_policy = "everything"
        root_dir = find_root_dir()
    else:
        # warn("Couldn't find existing kts project. Setting kaggle-mode")
        cache_mode = "ram"
        cache_policy = "service"
        root_dir = "."
    return cache_mode, cache_policy, root_dir


def check_structure(paths):
    """

    Args:
      paths: 

    Returns:

    """
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
    """ """
    paths = [
        "../input",
        "../notebooks",
        "../storage/info",
        "../storage/sources",
        "../output",
    ]

    if check_structure(paths):
        return

    raise ImportError(IMPORT_ERROR_MESSAGE)
