import os
import shutil
from .. import config

def list_files(startpath):
    for root, dirs, files in os.walk(startpath):
        level = root.replace(startpath, '').count(os.sep)
        indent = ' ' * 4 * level
        print('{}{}/'.format(indent, os.path.basename(root)))
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            print('{}{}'.format(subindent, f))


def check_existance(paths):
    """
    Checks necessity of clearing the folder.
    :param paths: list of directories
    :return: True if at least one directory exists, False otherwise
    """
    for path in paths:
        if os.path.isdir(path):
            return True
    return False


def check_structure(paths):
    for path in paths:
        if not os.path.isdir(path):
            return False
    return True


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


def build_file_system(force=False):
    """
    Builds directory structure for correct running kts.
    :param force: True or False (without confirmation or not)
    :return:
    """
    paths = ['./input', './notebooks', './storage/info', './storage/sources', './output']

    if check_existance(paths):
        if force:
            clear_all()

        list_files('./')
        print('Do you want to clear existing kts file system? [y/N]')
        try:
            answer = str(input())
            if answer.lower() == 'y' or answer.lower() == 'yes':
                clear_all()
        except Exception as e:
            raise TypeError('Invalid answer')

    print('Do you want to build the file system? [y/N]')
    try:
        answer = str(input())
        if answer.lower() == 'y' or answer.lower() == 'yes':
            for path in paths:
                if not os.path.isdir(path):
                    os.makedirs(path)
    except Exception as e:
        raise TypeError('Invalid answer')

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
