import inspect

from kts import config


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

