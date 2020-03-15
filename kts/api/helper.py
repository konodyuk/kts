import functools
import inspect

from kts.util.misc import extract_requirements, validate_source


class Helper:
    def __init__(self, func):
        self.func = func
        self.__name__ = self.func.__name__
        self.source = inspect.getsource(func)
        validate_source(self.source)
        self.requirements = extract_requirements(func)
        functools.update_wrapper(self, self.func)

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)
