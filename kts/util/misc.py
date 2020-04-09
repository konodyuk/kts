import inspect
import sys
from abc import ABCMeta
from types import FunctionType
from typing import Set

import importlib_metadata
from cloudpickle import dumps, loads

IGNORED_ATTRIBUTES = ['html', 'html_collapsible', '_html_elements']


class BadSignatureException(Exception):
    pass


def _create_source(class_name, bases, members):
    base_names = ", ".join([base.__name__ for base in bases])
    res = f"""class {class_name}({base_names}):\n"""
    for name, item in members.items():
        if name not in ["__module__", "__qualname__", "__doc__"] + IGNORED_ATTRIBUTES:
            if isinstance(item, FunctionType):
                res += inspect.getsource(item) + '\n'
            else:
                res += " " * 4 + f"{name} = {repr(item)}\n"
    return res.strip()


def _check_signatures(class_name, bases, members):
    for name, item in members.items():
        for base_class in bases:
            if name in base_class.__dict__ and name not in [
                "__module__",
                "__qualname__",
                "__doc__",
            ]:
                base_item = getattr(base_class, name)
                if not isinstance(item, FunctionType):
                    if isinstance(item, type(base_item)):
                        continue
                    else:
                        raise TypeError(f"Type discrepancy: {name} is of type {type(base_item)} in {base_class.__name__}, "
                                        f"but {type(item)} in {class_name}")
                sign = inspect.signature(item)
                base_sign = inspect.signature(base_item)
                if sign != base_sign:
                    raise BadSignatureException(
                        f"Signature discrepancy: {name}{sign} in {class_name}, "
                        f"but {name}{base_sign} in {base_class.__name__}")


class SourceMetaClass(ABCMeta):
    def __new__(cls, name, bases, members):
        if members != {'__doc__': None}:  # not in cloudpickle
            cls.check_methods(members)
            _check_signatures(name, bases, members)
            members["class_source"] = _create_source(name, bases, members)
        return ABCMeta.__new__(cls, name, bases, members)

    def check_methods(methods):
        pass


def getpackage(obj):
    module = inspect.getmodule(obj)
    if module is None:
        return None
    package_name = module.__name__.partition('.')[0]
    return sys.modules[package_name]


def getversion(module):
    try:
        return module.__version__
    except AttributeError:
        pass
    try:
        return importlib_metadata.version(module.__name__)
    except:
        return None


def extract_requirements(func) -> Set[str]:
    """TODO: extract PyPI name instead"""
    res = list()
    clean_namespace = loads(dumps(func)).__globals__
    for obj in clean_namespace.values():
        pkg = getpackage(obj)
        if pkg is not None:
            pkg_name = pkg.__name__
            pkg_version = getversion(pkg)
        else:
            pkg_name = None
            pkg_version = None
        if pkg_name in [None, '__main__', 'kts', 'numpy', 'pandas']:
            continue
        if pkg_version is not None:
            pkg_name += f"=={pkg_version}"
        res.append(pkg_name)
    return set(res)


def validate_source(source):
    tokens = source.split()
    assert 'import' not in tokens, "Runtime imports are not allowed, please move them to global scope."


class adaptivemethod(classmethod):
    def __get__(self, instance, owner):
        get = super().__get__ if instance is None else self.__func__.__get__
        return get(instance, owner)
