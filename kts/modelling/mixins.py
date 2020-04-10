from typing import Dict, Any

import numpy as np

import kts.ui.components as ui
from kts.util.hashing import hash_dict, hash_str


class TrackingMixin:
    @property
    def params(self) -> Dict[str, Any]:
        try:
            ignored_params = self.ignored_params
        except:
            ignored_params = self.get_ignored_params()
        params = {
            key: self.get_params()[key]
            for key in self.get_params() if key not in ignored_params
        }
        return params

    def format_params(self, prettify=False) -> str:
        if not prettify:
            return ', '.join(f"{k}={repr(v)}" for k, v in self.params.items())
        max_length = max(len(i) for i in self.params.keys())
        res = ""
        for key, value in self.params.items():
            res += key.rjust(max_length, ' ')
            res += f" = {repr(value)}\n"
        return res


class NamingMixin(TrackingMixin):
    @property
    def name(self):
        name = self.__class__.__name__
        name += hash_dict(self.params, 3)
        if 'class_source' in dir(self):
            name += hash_str(self.class_source, 2)
        return name


class SourceMixin(TrackingMixin):
    @property
    def source(self):
        return f"{self.__class__.__name__}({self.format_params()})"


class PreprocessingMixin:
    def preprocess(self, X, y=None):
        return X, y

    def preprocess_fit(self, X, y, *args, **kwargs):
        X_proc, y_proc = self.preprocess(X, y)
        self.fit(X_proc, y_proc, *args, **kwargs)

    def preprocess_predict(self, X, *args, **kwargs):
        X_proc, _ = self.preprocess(X, None)
        return self.predict(X_proc, *args, **kwargs)


class ProgressMixin:
    def progress_callback(self, line):
        return {'success': False}

    def enable_verbosity(self):
        pass

    def get_n_steps(self):
        return 1


class HTMLReprMixin(ui.HTMLRepr, TrackingMixin):
    def _html_elements(self):
        from kts.modelling.custom_model import CustomModel
        elements = [
            ui.Annotation('name'),
            ui.Field(self.name),
            ui.Annotation('model'),
            ui.Field(self.__class__.__name__),
            ui.Annotation('params'),
            ui.Code(self.format_params(prettify=True)),
            ui.Annotation('source'),
            ui.Code(self.source)
        ]
        if isinstance(self, CustomModel):
            elements += [
                ui.Annotation('custom model class source'),
                ui.Code(self.class_source)
            ]
        return elements

    @property
    def html(self):
        return ui.Column([ui.Title('model')] + self._html_elements()).html

    @property
    def html_collapsible(self):
        css_id = np.random.randint(1000000000)
        elements = [ui.TitleWithCross('model', css_id)]
        elements += self._html_elements()
        return ui.CollapsibleColumn(elements, ui.ThumbnailField('model', css_id), css_id).html


class Model(NamingMixin, SourceMixin, PreprocessingMixin, ProgressMixin, HTMLReprMixin):
    pass


class BinaryMixin(Model):
    def predict(self, X, **kwargs):
        return self.predict_proba(X, **kwargs)[:, 1]


class MulticlassMixin(Model):
    def predict(self, X, **kwargs):
        return self.predict_proba(X, **kwargs)


class RegressorMixin(Model):
    pass


class NormalizeFillNAMixin(PreprocessingMixin):
    def preprocess(self, X, y=None):
        if y is not None:
            mean = np.nanmean(X, axis=0)
            std = np.nanstd(X, axis=0) + 1e-9
            X = (X - mean) / std
            self._preprocessing_state = mean, std
        else:
            mean, std = self._preprocessing_state
            X = (X - mean) / std

        nan_idx = np.where(np.isnan(X) | np.isinf(X))
        X[nan_idx] = np.take(mean, nan_idx[1])
        return X, y


class Placeholder:
    def __init__(self):
        raise ImportError(f'{self.__class__} is not available. Please install corresponding packages')
