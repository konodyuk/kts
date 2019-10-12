import inspect
import os

from .. import config


def get_source(function):
    """Gets source code of a function. Ignores decorators starting with @.

    Args:
      function: function

    Returns:
      source code

    """
    src = inspect.getsource(function)
    if src[0] == "@":
        src = src[src.find("\n") + 1:]
    return src


def shorten(source):
    """

    Args:
      source: 

    Returns:

    """
    if len(source) <= config.MAX_LEN_SOURCE:
        return source
    # source = source.replace('columns=', '')
    left = source[:config.MAX_LEN_SOURCE // 2]
    left = left[:left.rfind(" ")]
    right = source[-config.MAX_LEN_SOURCE // 2:]
    right = right[right.find(" "):]
    return left + " ..." + right


def source_path(function):
    """

    Args:
      function: 

    Returns:

    """
    return config.source_path + f"{function.__name__}.py"


def save_source(function):
    """

    Args:
      function: 

    Returns:

    """
    path = source_path(function)
    with open(path, "w") as f:
        f.write(get_source(function))


def source_is_saved(function):
    """

    Args:
      function: 

    Returns:

    """
    return os.path.exists(source_path(function))


def load_source(function):
    """

    Args:
      function: 

    Returns:

    """
    if not source_is_saved(function):
        raise FileNotFoundError
    with open(source_path(function), "r") as f:
        source = f.read()
    return source


def matches_cache(function):
    """

    Args:
      function: 

    Returns:

    """
    return get_source(function) == load_source((function))
