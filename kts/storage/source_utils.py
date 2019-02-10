import inspect
from .. import config
import os


def get_source(function):
    """
    Gets source code of a function. Ignores decorators starting with @.
    :param function: function
    :return: source code
    """
    src = inspect.getsource(function)
    if src[0] == '@':
        src = src[src.find('\n') + 1:]
    return src


def source_path(function):
    return config.source_path + f"{function.__name__}.py"


def save_source(function):
    path = source_path(function)
    with open(path, 'w') as f:
        f.write(get_source(function))


def source_is_saved(function):
    return os.path.exists(source_path(function))


def load_source(function):
    if not source_is_saved(function):
        raise FileNotFoundError
    with open(source_path(function), 'r') as f:
        source = f.read()
    return source


def matches_cache(function):
    return get_source(function) == load_source((function))
