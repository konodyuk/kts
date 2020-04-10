import time
from contextlib import redirect_stdout
from typing import List, Dict, Union

from IPython.display import display

from kts.settings import cfg
from kts.ui.components import HTMLRepr, Field, Annotation, AlignedColumns, Output, Progress, InnerColumn, Row, Title, \
    Column
from kts.util.formatting import format_value


class Importance(HTMLRepr):
    def __init__(self, value, vmin, vmax, zero):
        self.value = value
        self.vmin = vmin
        self.vmax = vmax
        self.zero = zero

    @property
    def html(self):
        if self.value >= self.zero:
            hbar_style = f'width: {self.value - self.zero}px; left: {self.zero}px; height: 1em; top: calc(50% - 0.5em);'
        else:
            hbar_style = f'width: {self.zero - self.value}px; left: {self.value}px; height: 1em; top: calc(50% - 0.5em);'
        return f'''<div class="kts-hbar-container" style="width: {self.vmax}px">
        <div class="kts-hbar" style="{hbar_style}"></div>
        {f'<div class="kts-hbar-line" style="width: 1px; left: {self.zero}px; height: calc(100% + 8px); top: -4px;"></div>' if self.zero else ''}
        <div class="kts-hbar-line" style="width: {self.vmax - self.vmin}px; left: {self.vmin}px;"></div>
        <div class="kts-hbar-line" style="width: 1px; height: 5px; top: calc(50% - 2px); left: {self.vmin}px"></div>
        <div class="kts-hbar-line" style="width: 1px; height: 5px; top: calc(50% - 2px); left: {self.vmax}px"></div>
        </div>
        '''


class FeatureImportances(HTMLRepr):
    def __init__(self, features: List[Dict[str, Union[str, 'FeatureConstructor', float]]], width: int = 450):
        self.features = features
        self.width = width
        self.max_importance = max([i['max'] for i in self.features])
        self.min_importance = min([i['min'] for i in self.features])
        self.zero_position = 0
        if self.min_importance < 0:
            self.zero_position = self.to_px(0)
        else:
            self.min_importance = 0

    def to_px(self, value):
        if self.max_importance == self.min_importance:
            return 0
        return self.width * ((value - self.min_importance) / (self.max_importance - self.min_importance))

    @property
    def html(self):
        name_annotation = Annotation('feature', style="text-align: right; margin-bottom: 3px; margin-right: 5px;")
        mean_annotation = Annotation('mean', style="margin-left: 7px; margin-bottom: 3px;")
        hbar_annotation = Annotation('importance', style="margin-left: 5px; margin-bottom: 3px;")
        # names = [i['feature_constructor'].html_collapsible(name=i['name'], style='padding: 0px 5px; text-align: right;', bg=False, border=True) for i in self.features]
        names = [Field(i['name'], style='padding: 0px 5px; text-align: right; margin: 2px;', bg=False).html for i in self.features]
        means = [Field(format_value(i['mean']), style='padding: 0px 5px; max-height: 1.5em; margin: 2px;', bg=False).html for i in self.features]
        hbars = [Importance(self.to_px(i['mean']), self.to_px(i['min']), self.to_px(i['max']), self.zero_position).html for i in self.features]
        return AlignedColumns([
            [name_annotation.html] + names,
            [mean_annotation.html] + means,
            [hbar_annotation.html] + hbars,
        ], title='feature importances').html


class Hbar(HTMLRepr):
    def __init__(self, width):
        self.width = width

    @property
    def html(self):
        return f'<div class="kts-hbar" style="width: {self.width}px"></div>'


class ImportanceComputingReport(HTMLRepr):
    update_interval = 0.1

    def __init__(self, total):
        self.total = total
        self.last_update = 0
        self.step = 1
        self.current_feature = 'nothing'
        self.start = None
        self.took = None
        self.eta = None
        self.handle = None

    def update(self, current_feature):
        self.step += 1
        if self.start is None:
            self.start = time.time()
        else:
            self.took = time.time() - self.start
            self.eta = (self.total - self.step) / max(self.step, 1) * self.took
        self.current_feature = current_feature
        self.refresh()

    def show(self):
        return display(self, display_id=True)

    def refresh(self, force=False):
        if self.handle is None:
            with redirect_stdout(cfg.stdout):
                self.handle = self.show()
        if self.handle is None:
            # not in ipython
            return
        if force or time.time() - self.last_update >= self.update_interval:
            with redirect_stdout(cfg.stdout):
                self.handle.update(self)
            self.last_update = time.time()

    @property
    def html(self):
        ind_kw = dict(style='width: 4.5em; padding-top: 0px; padding-bottom: 0px;', bg=False, bold=True)
        return Column([
            Title('computing importances'),
            Row([
                InnerColumn([
                    Annotation('progress'),
                    Progress(self.step, self.total, style='width: 500px; margin-top: 7px;'),
                    Output(f'Computing {self.current_feature}')
                ]),
                InnerColumn([
                    Annotation('took'),
                    Field(format_value(self.took, time=True), **ind_kw)
                ]),
                InnerColumn([
                    Annotation('eta'),
                    Field(format_value(self.eta, time=True), **ind_kw)
                ]),
            ])
        ]).html
