import hashlib
import inspect
from .feature.storage import FeatureConstructor


def list_hash(lst, length):
    return hashlib.sha256(repr(tuple(lst)).encode()).hexdigest()[:length]


def extract_signature(func):
    args = inspect.getargspec(func).args
    defaults = inspect.getargspec(func).defaults
    values = inspect.currentframe().f_back.f_locals
    print(defaults)
    sources = []
    for arg in args[:-len(defaults)]:
        sources.append(f'{arg}={repr(values[arg])}')
    for arg, default in zip(args[-len(defaults):], defaults):
        if values[arg] != default:
            sources.append(f'{arg}={repr(values[arg])}')
    return ', '.join(sources)


def wrap_stl_function(outer_function, inner_function):
    source = f'{outer_function.__name__}({extract_signature(outer_function)})'
    fc = FeatureConstructor(inner_function, cache_default=False, stl=True)
    fc.source = source
    return fc
