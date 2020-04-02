from IPython.display import display

from kts.settings import cfg
from kts.ui.components import Column, Pool, HTMLRepr, Title, Annotation, CSS, Raw, Field


class Dashboard(HTMLRepr):
    def __init__(self):
        from kts.core.lists import feature_list, helper_list
        self.features = feature_list
        self.helpers = helper_list

    @property
    def html(self):
        elements = [Title('dashboard')]
        if self.features:
            elements += [Annotation('features'), Pool([Raw(i.html_collapsible()) for i in self.features.values()])]
        else:
            elements += [Annotation('features'), Field("You've got no features so far.")]
        if self.helpers:
            elements += [Annotation('helpers'), Pool([Raw(i.html_collapsible()) for i in self.helpers.values()])]
        else:
            elements += [Annotation('helpers'), Field("You've got no helpers so far.")]
        # TODO: add 5 best/recent experiments from lb
        elements += [CSS(cfg._theme.css), CSS(cfg._highlighter.css)]
        return Column(elements).html


def dashboard():
    cfg._dashboard_handles.append(display(Dashboard(), display_id=True))
