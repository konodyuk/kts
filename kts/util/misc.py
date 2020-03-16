import inspect
import sys
from abc import ABCMeta
from typing import Set

import importlib_metadata
from cloudpickle import dumps, loads

IGNORED_ATTRIBUTES = ['html', 'html_collapsible', '_html_elements']


class BadSignatureException(Exception):
    pass


def _create_source(class_name, base_classes, methods):
    base_class_names = ", ".join([bc.__name__ for bc in base_classes])
    res = f"""class {class_name}({base_class_names}):\n"""
    for name, meth in methods.items():
        if name not in ["__module__", "__qualname__", "__doc__"] + IGNORED_ATTRIBUTES:
            try:
                res += inspect.getsource(meth) + "\n"
            except TypeError:
                raise UserWarning(
                    f"Unexpected static variable in class definition: {name}. Use instance variables instead."
                )
    return res.strip()


def _check_signatures(class_name, base_classes, methods):
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
    def __new__(cls, class_name, base_classes, dict_of_methods):
        cls.check_methods(dict_of_methods)
        _check_signatures(class_name, base_classes, dict_of_methods)
        dict_of_methods["class_source"] = _create_source(class_name, base_classes, dict_of_methods)
        return ABCMeta.__new__(cls, class_name, base_classes, dict_of_methods)

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
