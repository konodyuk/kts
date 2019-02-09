import os
import shutil


def check_existance(paths):
    """
    Checks necessity clearing the folder.
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


def build_file_system(force=False):
    """
    Builds directory structure for correct running kts.
    :param force: True or False (without confirmation or not)
    :return:
    """
    paths = ['./input', './notebooks', './storage/info', './storage/sources']

    if check_existance(paths):
        if force:
            clear_all()

        print('Do you want to clear existing file system? (y/n)')
        try:
            answer = str(input())
            if answer == 'y':
                clear_all()
        except Exception as e:
            raise TypeError('Invalid answer')

    for path in paths:
        if not os.path.isdir(path):
            os.makedirs(path)
