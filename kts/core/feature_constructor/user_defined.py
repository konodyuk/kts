import inspect

import numpy as np

import kts.ui.components as ui
from kts.core.backend.run_manager import run_cache
from kts.core.feature_constructor.parallel import ParallelFeatureConstructor
from kts.core.frame import KTSFrame


class FeatureConstructor(ParallelFeatureConstructor, ui.HTMLRepr):
    parallel = True
    cache = True

    def __init__(self, func, internal=False):
        self.func = func
        if internal:
            return
        self.name = func.__name__
        self.description = func.__doc__
        self.source = inspect.getsource(func)
        self.dependencies = self.extract_dependencies(func)
        self.registered = True

    def compute(self, kf: KTSFrame):
        kwargs = {key: self.request_resource(value, kf) for key, value in self.dependencies}
        result = self.func(kf, **kwargs)
        if (not kf.train and '__columns' in kf._state
            and not (len(result.columns) == len(kf._state['__columns'])
                     and all(result.columns == kf._state['__columns']))):
            fixed_columns = kf._state['__columns']
            for col in set(fixed_columns) - set(result.columns):
                result[col] = None
            return result[fixed_columns]
        if '__columns' not in kf._state:
            kf._state['__columns'] = list(result.columns)
        return result

    def extract_dependencies(self, func):
        dependencies = dict()
        for k, v in inspect.signature(func).parameters.items():
            if isinstance(v.default, str):
                dependencies[k] = v.default
            elif v.default != inspect._empty:
                raise UserWarning(f"Unsupported argument type: {k}={type(v.default)}. String values expected.")
        return dependencies

    @property
    def columns(self):
        return run_cache.get_columns(self.name)

    def _html_elements(self):
        elements = [ui.Annotation('name'), ui.Field(self.name)]
        if self.description is not None:
            elements += [ui.Annotation('description'), ui.Field(self.description)]
        elements += [ui.Annotation('source'), ui.Code(self.source)]
        if self.columns:
            elements += [ui.Annotation('columns'), ui.Field(', '.join(self.columns))]
        # if self.preview_df is not None:
        #     elements += [ui.Annotation('preview'), ui.DF(self.preview_df)]
        return elements

    @property
    def html(self):
        elements = [ui.Title('feature constructor')]
        elements += self._html_elements()
        return ui.Column(elements).html

    def html_collapsible(self, name=None, style="", border=False, **kw):
        if name is None:
            name = self.name
        css_id = np.random.randint(1000000000)
        elements = [ui.TitleWithCross('feature constructor', css_id)]
        elements += self._html_elements()
        thumbnail = ui.ThumbnailField(name, css_id, style=style, **kw)
        return ui.CollapsibleColumn(elements, thumbnail, css_id, border=border).html
