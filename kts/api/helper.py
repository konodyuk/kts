import functools
import inspect

import numpy as np

import kts.ui.components as ui
from kts.util.misc import extract_requirements, validate_source


class Helper(ui.HTMLRepr):
    def __init__(self, func):
        self.func = func
        self.name = self.func.__name__
        self.source = inspect.getsource(func)
        validate_source(self.source)
        self.requirements = extract_requirements(func)
        functools.update_wrapper(self, self.func)

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def __repr__(self):
        return self.name

    def _html_elements(self):
        elements = [ui.Annotation('name'), ui.Field(repr(self))]
        elements += [ui.Annotation('source'), ui.Code(self.source)]
        if self.requirements:
            elements += [ui.Annotation('requirements'), ui.Field('<tt>' + ', '.join(self.requirements) + '</tt>')]
        return elements

    @property
    def html(self):
        elements = [ui.Title('helper')]
        elements += self._html_elements()
        return ui.Column(elements).html

    def html_collapsible(self, name=None, style="", border=False, **kw):
        if name is None:
            name = repr(self)
        css_id = np.random.randint(1000000000)
        elements = [ui.TitleWithCross('helper', css_id)]
        elements += self._html_elements()
        thumbnail = ui.ThumbnailField(name, css_id, style=style, **kw)
        return ui.CollapsibleColumn(elements, thumbnail, css_id, border=border).html
