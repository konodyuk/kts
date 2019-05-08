import hashlib
import inspect
from itertools import zip_longest
import numpy as np
import time


def captcha():
    np.random.seed(int(time.time()))
    a, b = np.random.randint(5, 30, size=2)
    c = int(input(f"{a} + {b} = "))
    if a + b != c:
        return False
    return True


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


