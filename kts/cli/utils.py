import os
import shutil

import pkg_resources


def find_root_dir():
    i = 0
    while os.path.realpath("../" * i) != "/":
        if os.path.exists("../" * i + "kts_config.py"):
            if i != 0:
                return "../" * i
            else:
                return "./"
        i += 1
    return False


def get_mode():
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


def list_files(startpath):
    for root, dirs, files in os.walk(startpath):
        level = root.replace(startpath, '').count(os.sep)
        indent = ' ' * 4 * level
        print('{}{}/'.format(indent, os.path.basename(root)))
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            print('{}{}'.format(subindent, f))


def check_existence(paths):
    """
    Checks necessity of clearing the folder.
    :param paths: list of directories
    :return: True if at least one directory exists, False otherwise
    """
    for path in paths:
        if os.path.isdir(path):
            return True
    return False


def clear_all():
    """
    Clears current folder.
    :return:
    """
    folder = './'
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(e)


def create_config():
    shutil.copy(pkg_resources.resource_filename('kts', 'cli/default_config.toml'), 'kts_config.toml')
