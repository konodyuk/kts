import inspect
from abc import ABCMeta


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
