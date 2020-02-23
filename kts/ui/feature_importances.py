from typing import List, Dict, Union

from kts.ui.components import HTMLRepr, Field, Annotation, AlignedColumns


class Importance(HTMLRepr):
    def __init__(self, value, vmin, vmax, zero):
        self.value = value
        self.vmin = vmin - 1
        self.vmax = vmax - 1
        self.zero = zero

    @property
    def html(self):
        if self.value >= self.zero:
            hbar_style = f'width: {self.value - self.zero}px; left: {self.zero}px;'
        else:
            hbar_style = f'width: {self.zero - self.value}px; left: {self.value}px;'
        return f'''<div class="hbar-container" style="width: {self.vmax}px">
        <div class="hbar" style="{hbar_style}"></div>
        {f'<div class="hbar-line" style="width: 1px; left: {self.zero}px; height: calc(100% + 8px); top: -4px;"></div>' if self.zero else ''}
        <div class="hbar-line" style="width: {self.vmax - self.vmin}px; left: {self.vmin}px"></div>
        <div class="hbar-line" style="width: 1px; height: 5px; top: calc(50% - 2px); left: {self.vmin}px"></div>
        <div class="hbar-line" style="width: 1px; height: 5px; top: calc(50% - 2px); left: {self.vmax}px"></div>
        </div>
        '''


class FeatureImportances(HTMLRepr):
    def __init__(self, features: List[Dict[str, Union[str, 'FeatureConstructor', float]]], width: int = 600):
        self.features = features
        self.width = width
        self.max_importance = max([i['max'] for i in self.features])
        self.min_importance = min([i['min'] for i in self.features])
        self.zero_position = 0
        if self.min_importance < 0:
            self.zero_position = self.to_px(0)

    def to_px(self, value):
        return self.width * ((value - self.min_importance) / (self.max_importance - self.min_importance))

    @property
    def html(self):
        name_annotation = Annotation('feature', style="text-align: right; margin-bottom: 3px; margin-right: 5px;")
        mean_annotation = Annotation('mean', style="margin-left: 7px; margin-bottom: 3px;")
        hbar_annotation = Annotation('importance', style="margin-left: 5px; margin-bottom: 3px;")
        names = [i['feature_constructor'].html_collapsable(name=i['name'], style='padding: 0px 5px; text-align: right;', bg=False, border=True) for i in self.features]
        means = [Field(i['mean'], style='max-height: 1.5rem; margin: 2px;', bg=False).html for i in self.features]
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
        return f'<div class="hbar" style="width: {self.width}px"></div>'
