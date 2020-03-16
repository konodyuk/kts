import time
from typing import Union

import numpy as np
import pandas as pd

import kts.ui.components as ui
from kts.feature_selection import Builtin
from kts.ui.feature_importances import FeatureImportances
from kts.util.hashing import hash_str


def experiment_id(model, feature_set):
    return hash_str(model.name + feature_set.name, 6)


class Experiment(ui.HTMLRepr):
    def __init__(
        self, 
        cv_pipeline, 
        oof, 
        description,
        validator
    ):
        self.cv_pipeline = cv_pipeline
        self.oof = oof
        self.description = description
        self.validator = validator
        self.id = experiment_id(self.model, self.feature_set)
        self.date = time.time()
        if oof.shape[1] == 1:
            self.oof.columns = [self.id]
        else:
            self.oof.columns = [f"{self.id}_{i}" for i in range(oof.shape[1])]

    @property
    def model(self):
        return self.cv_pipeline.model

    @property
    def model_class(self):
        return self.model.__class__.__name__

    @property
    def feature_set(self):
        return self.cv_pipeline.feature_set
    
    @property
    def score(self):
        return np.mean(self.cv_pipeline.scores)
    
    @property
    def std(self):
        return np.std(self.cv_pipeline.scores)

    @property
    def took(self):
        return self.cv_pipeline.took
    
    def predict(self, frame):
        return self.cv_pipeline.predict(frame)

    def feature_importances(self, plot=True, estimator=Builtin(), sort_by='max', head=None) -> Union[pd.DataFrame, FeatureImportances]:
        importances_by_fold = pd.DataFrame()
        cv_feature_set = self.cv_pipeline.cv_feature_set
        for i in range(cv_feature_set.n_folds):
            fold = cv_feature_set.fold(i)
            model = self.cv_pipeline.models[i]
            importances_by_fold = importances_by_fold.append(estimator.estimate(fold, model), ignore_index=True)
        if not plot:
            return importances_by_fold
        payload = list(importances_by_fold
                       .agg(['name', 'min', 'max', 'mean'])
                       .sort_values(axis=1, by=sort_by, ascending=False)
                       .to_dict()
                       .values())
        if head:
            payload = payload[:head]
        return FeatureImportances(payload)

    def move_to(self, lb_name: str):
        raise NotImplemented

    @property
    def alias(self):
        return ExperimentAlias(self)

    @property
    def _html_elements(self):
        elements = [
            ui.Annotation('ID'),
            ui.Field(self.id),
            ui.Annotation('score'),
            ui.Field(self.score),
            ui.Annotation('description'),
            ui.Field(self.description),
            ui.Annotation('model'),
            ui.Field(self.model_class),
            ui.Annotation('params'),
            ui.Code(self.model.format_params(prettify=True)),
            ui.Annotation('features'),
            self.feature_set.feature_pool,
            ui.Annotation('details'),
            ui.Pool([
                ui.Raw(self.feature_set.html_collapsible),
                ui.Raw(self.model.html_collapsible),
                ui.Raw(self.validator.html_collapsible)
            ])
        ]
        if self.requirements:
            elements += [ui.Annotation('requirements'), ui.Field('<tt>' + ', '.join(self.requirements) + '</tt>')]
        return elements
    
    @property
    def html(self):
        return ui.Column([ui.Title('experiment')] + self._html_elements).html
    
    def html_collapsible(self, thumbnail=None, border=False):
        css_id = np.random.randint(1000000000)
        if thumbnail is None:
            thumbnail = ui.ThumbnailField(f"{self.id}", css_id)
        else:
            thumbnail.css_id = css_id
        elements = [ui.TitleWithCross('experiment', css_id)]
        elements += self._html_elements
        return ui.CollapsibleColumn(elements, thumbnail, css_id, outer=True, border=border).html

    @property
    def requirements(self):
        return self.feature_set.requirements


class ExperimentAlias(ui.HTMLRepr):
    def __init__(self, experiment):
        self._html_elements = experiment._html_elements
        self.id = experiment.id
        self.score = experiment.score
        self.model_class = experiment.model_class
        self.n_features = len(experiment.feature_set.before_split) + len(experiment.feature_set.before_split)
        self.date = experiment.date
        self.took = experiment.took

    @property
    def html(self):
        return ui.Column([ui.Title('experiment')] + self._html_elements).html
    
    def html_collapsible(self, thumbnail=None, border=False):
        css_id = np.random.randint(1000000000)
        if thumbnail is None:
            thumbnail = ui.ThumbnailField(f"{self.id}", css_id)
        else:
            thumbnail.css_id = css_id
        elements = [ui.TitleWithCross('experiment', css_id)]
        elements += self._html_elements
        return ui.CollapsibleColumn(elements, thumbnail, css_id, outer=True, border=border).html
