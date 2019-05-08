import warnings
import re
from ..storage import cache


def is_identifier(string):
    return isinstance(string, str) and bool(re.match('[0-9A-F]{6}', string))


def is_list_of_identifiers(a):
    if not isinstance(a, list):
        return False
    for i in a:
        if not is_identifier(i):
            return False
    return True


def get_experiment(identifier):
    assert is_identifier(identifier), 'You can only get experiment by ID'
    names = [name for name in cache.cached_objs() if name.endswith('_exp') and name.startswith(identifier)]
    if len(names) == 0:
        raise KeyError('No such experiment found')
    elif len(names) == 1:
        return cache.load_obj(names[0])
    else:
        warnings.warn("I don't know what happened but there are several objects with this ID")
        return [cache.load_obj(name) for name in names]

