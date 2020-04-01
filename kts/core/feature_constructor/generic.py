import inspect
from copy import copy
from functools import wraps
from inspect import Signature, Parameter

import numpy as np

import kts.ui.components as ui
from kts.core.feature_constructor.user_defined import FeatureConstructor
from kts.util.misc import extract_requirements, validate_source


class GenericFeatureConstructor(ui.HTMLRepr):
    def __init__(self, func, kwargs):
        self.func = func
        self.name = func.__name__
        self.description = func.__doc__
        self.source = inspect.getsource(func)
        validate_source(self.source)
        self.requirements = extract_requirements(func)
        self.parallel = kwargs.pop('parallel', True)
        self.cache = kwargs.pop('cache', True)
        self.verbose = kwargs.pop('verbose', True)
        self.arg_names = list(kwargs.keys())
        self.kwargs = kwargs
        parameters = [Parameter(p, kind=Parameter.POSITIONAL_OR_KEYWORD, default=kwargs[p]) for p in self.arg_names]
        self.__class__.__call__.__signature__ = Signature(
            parameters=[Parameter('self', kind=Parameter.POSITIONAL_ONLY)] + parameters)

    def call(self, *args, **kwargs):
        instance_kwargs = copy(self.kwargs)
        for k, v in zip(self.arg_names, args):
            instance_kwargs[k] = v
            if k in kwargs:
                raise TypeError(f"{self.name}() got multiple values for argument {repr(k)}")
        for k, v in kwargs.items():
            if k not in self.kwargs:
                raise ValueError(f"Unexpected arg: {k}")
            instance_kwargs[k] = v
        res = FeatureConstructor(self.modify(self.func, instance_kwargs), internal=True)
        res.name = f"{self.name}__" + "_".join(map(str, instance_kwargs.values()))
        res.description = f"An instance of generic feature constructor <tt>{self.name}</tt>"
        res.source = f"{self.name}({', '.join(f'{repr(instance_kwargs[k])}' for k in self.arg_names)})"
        res.additional_source = self.source
        # res.source = f"{self.name}({', '.join(f'{k}={repr(v)}' for k, v in instance_kwargs.items())})"
        res.requirements = self.requirements
        res.dependencies = dict()
        res.parallel = self.parallel
        res.cache = self.cache
        res.verbose = self.verbose
        return res

    def modify(self, func, instance_kwargs):
        @wraps(func)
        def new_func(*args, **kwargs):
            g = func.__globals__
            old_values = dict()
            placeholder = object()
            for k, v in instance_kwargs.items():
                old_values[k] = g.get(k, placeholder)
                g[k] = v

            try:
                res = func(*args, **kwargs)
            finally:
                for k, v in old_values.items():
                    if v is placeholder:
                        del g[k]
                    else:
                        g[k] = v
            return res
        return new_func

    def _html_elements(self):
        elements = [ui.Annotation('name'), ui.Field(self.name)]
        if self.description is not None:
            elements += [ui.Annotation('description'), ui.Field(self.description)]
        elements += [ui.Annotation('source'), ui.Code(self.source)]
        if self.requirements:
            elements += [ui.Annotation('requirements'), ui.Field('<tt>' + ', '.join(self.requirements) + '</tt>')]
        return elements

    @property
    def html(self):
        elements = [ui.Title('generic feature')]
        elements += self._html_elements()
        return ui.Column(elements).html

    def html_collapsible(self, name=None, style="", border=False, **kw):
        if name is None:
            name = self.name
        css_id = np.random.randint(1000000000)
        elements = [ui.TitleWithCross('generic feature', css_id)]
        elements += self._html_elements()
        thumbnail = ui.ThumbnailField(name, css_id, style=style, **kw)
        return ui.CollapsibleColumn(elements, thumbnail, css_id, border=border).html


def create_generic(func, kwargs):
    def new_call(self, *args, **kwargs):
        return self.call(*args, **kwargs)
    members = dict(__call__=new_call)
    generic_type = type(f"Generic({func.__name__})", (GenericFeatureConstructor,), members)
    return generic_type(func, kwargs)
