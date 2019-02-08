import os
import shutil


def check_existance(paths):
    for path in paths:
        if os.path.isdir(path):
            return True
    return False


def clear_all():
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


def build_file_system():
    paths = ['./input', './notebooks', './storage/info', './storage/sources']

    if check_existance(paths):
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
