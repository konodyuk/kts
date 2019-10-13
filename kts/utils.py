import hashlib
import inspect
import time
from abc import ABCMeta
from itertools import zip_longest

import numpy as np


def captcha():
    """ """
    np.random.seed(int(time.time()))
    a, b = np.random.randint(5, 30, size=2)
    c = int(input(f"{a} + {b} = "))
    if a + b != c:
        return False
    return True


def list_hash(lst, length):
    """

    Args:
      lst: 
      length: 

    Returns:

    """
    return hashlib.sha256(repr(tuple(lst)).encode()).hexdigest()[:length]


def hash_str(a):
    """

    Args:
      a: 

    Returns:

    """
    return hashlib.sha256(a.encode()).hexdigest()


def extract_signature(func, return_dict=False):
    """

    Args:
      func: 
      return_dict:  (Default value = False)

    Returns:

    """
    args = inspect.getfullargspec(func).args
    defaults = inspect.getfullargspec(func).defaults
    values = {
        **inspect.currentframe().f_back.f_locals,
        **inspect.currentframe().f_back.f_back.f_locals,
    }
    if defaults is None:
        defaults = []
    if args is None:
        args = []
    # print(inspect.getfullargspec(func))
    # print(values)
    if return_dict:
        return {arg: values[arg] for arg in args}
    sources = []
    for arg, default in list(zip_longest(args[::-1], defaults[::-1]))[::-1]:
        if values[arg] != default:
            if is_helper(values[arg]):
                arg_repr = values[arg].__name__
            else:
                arg_repr = repr(values[arg])
            sources.append(f"{arg}={arg_repr}")
    return ", ".join(sources)


def is_helper(func):
    """

    Args:
      func: 

    Returns:

    """
    return (callable(func) and "__name__" in dir(func)
            and "source" in dir(func) and isinstance(func.source, str)
            and "def" in func.source)  # genius


class BadSignatureException(Exception):
    """ """

    pass


def _create_source(class_name, base_classes, methods):
    """

    Args:
      class_name: 
      base_classes: 
      methods: 

    Returns:

    """
    base_class_names = ", ".join([bc.__name__ for bc in base_classes])
    res = f"""class {class_name}({base_class_names}):\n"""
    for name, meth in methods.items():
        if name not in ["__module__", "__qualname__", "__doc__"]:
            try:
                res += inspect.getsource(meth) + "\n"
            except TypeError:
                raise UserWarning(
                    f"Unexpected static variable in class definition: {name}. Use instance variables instead."
                )
    return res.strip()


def _check_signatures(class_name, base_classes, methods):
    """

    Args:
      class_name: 
      base_classes: 
      methods: 

    Returns:

    """
    for name, meth in methods.items():
        for base_class in base_classes:
            if name in base_class.__dict__ and name not in [
                "__module__",
                "__qualname__",
                "__doc__",
            ]:
                base_meth = getattr(base_class, name)
                sign = inspect.signature(meth)
                base_sign = inspect.signature(base_meth)
                if sign != base_sign:
                    raise BadSignatureException(
                        f"Signature discrepancy: {name}{sign} in {class_name}, "
                        f"but {name}{base_sign} in {base_class.__name__}")


class SourceMetaClass(ABCMeta):
    """ """
    def __new__(cls, class_name, base_classes, dict_of_methods):
        cls.check_methods(dict_of_methods)
        _check_signatures(class_name, base_classes, dict_of_methods)
        dict_of_methods["class_source"] = _create_source(
            class_name, base_classes, dict_of_methods)
        return type.__new__(cls, class_name, base_classes, dict_of_methods)

    def check_methods(methods):
        """

        Args:
          methods: 

        Returns:

        """
        pass
