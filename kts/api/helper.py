import functools
import inspect


class Helper:
    def __init__(self, func):
        self.func = func
        self.__name__ = self.func.__name__
        self.source = inspect.getsource(func)
        functools.update_wrapper(self, self.func)

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)
