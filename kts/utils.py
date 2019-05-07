import hashlib
import inspect
from .feature.storage import FeatureConstructor
from itertools import zip_longest


def list_hash(lst, length):
    return hashlib.sha256(repr(tuple(lst)).encode()).hexdigest()[:length]


def hash_str(a):
    return hashlib.sha256(a.encode()).hexdigest()


def extract_signature(func):
    args = inspect.getfullargspec(func).args
    defaults = inspect.getfullargspec(func).defaults
    values = {**inspect.currentframe().f_back.f_locals, **inspect.currentframe().f_back.f_back.f_locals}
    if defaults is None:
        defaults = []
    if args is None:
        args = []
    # print(inspect.getfullargspec(func))
    # print(values)
    sources = []
    for arg, default in list(zip_longest(args[::-1], defaults[::-1]))[::-1]:
        if values[arg] != default:
            sources.append(f'{arg}={repr(values[arg])}')
    return ', '.join(sources)


def wrap_stl_function(outer_function, inner_function):
    source = f'stl.{outer_function.__name__}({extract_signature(outer_function)})'
    fc = FeatureConstructor(inner_function, cache_default=False, stl=True)
    fc.source = source
    return fc
