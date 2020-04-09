from typing import Optional

import numpy as np
import pandas as pd

import kts.ui.components as ui
from kts.core.feature_set import FeatureSet
from kts.modelling.mixins import Model
from kts.modelling.pipeline import CVPipeline
from kts.modelling.pipeline import pipeline_id
from kts.settings import cfg
from kts.ui.feature_computing_report import FeatureComputingReport
from kts.util.misc import SourceMetaClass
from kts.validation.experiment import Experiment
from kts.validation.leaderboard import leaderboard_list, experiments


class Validator(ui.HTMLRepr, metaclass=SourceMetaClass):
    def __init__(self, splitter, metric):
        self.splitter = splitter
        self.metric = metric

    def create_folds(self, feature_set, splitter):
        y = feature_set.target.values
        for idx_train, idx_test in splitter.split(y, y):
            yield idx_train, idx_test

    def create_oof(self, predictions, folds, feature_set):
        input_frame = feature_set.train_frame
        height = input_frame.shape[0]
        pred_sample = predictions[0]
        if len(pred_sample.shape) == 1 or pred_sample.shape[1] == 1:
            width = 1
        else:
            width = pred_sample.shape[1]
        res = np.zeros((height, width))
        weights = np.zeros((height, 1))
        for pred, (idx_train, idx_val) in zip(predictions, folds):
            if len(pred.shape) == 1:
                pred = pred.reshape((-1, 1))
            res[idx_val] += pred
            weights[idx_val] += 1
        res = res / weights
        res = pd.DataFrame(res, index=input_frame.index.copy())
        res = res[weights > 0]
        return res

    def evaluate(self, y_true, y_pred, fold_feature_set):
        return self.metric(y_true, y_pred)

    def score(self, model: Model, feature_set: FeatureSet, desc: Optional[str] = None, leaderboard: str = "main", **kwargs):
        cfg.preview_mode = False
        if pipeline_id(model, feature_set) in experiments:
            raise UserWarning(f'Duplicate experiment: {pipeline_id(model, feature_set)}')
        report = FeatureComputingReport()
        feature_set.compute(report=report)
        folds = self.create_folds(feature_set, self.splitter)
        folds = list(folds)
        cv_feature_set = feature_set.split(folds)
        cv_feature_set.compute(report=report)
        cv_pipeline = CVPipeline(cv_feature_set, model)
        cv_pipeline.fit(score_fun=self.evaluate, **kwargs)
        raw_oof = cv_pipeline.raw_oof
        oof = self.create_oof(raw_oof, folds, feature_set)
        cv_pipeline.raw_oof = None
        cv_pipeline.compress()
        experiment = Experiment(
            cv_pipeline=cv_pipeline,
            oof=oof,
            description=desc,
            validator=self,
        )
        leaderboard_list.register(experiment, leaderboard)
        cfg.preview_mode = True
        return {'score': experiment.score, 'id': experiment.id}

    def _html_elements(self):
        elements = [
            ui.Annotation('splitter'),
            ui.Code(repr(self.splitter)),
            ui.Annotation('metric'),
            ui.Code(self.metric.__name__)  # should also consider metrics defined as helpers
        ]
        return elements

    @property
    def html(self):
        return ui.Column([ui.Title('validator')] + self._html_elements()).html

    @property
    def html_collapsible(self):
        css_id = np.random.randint(1000000000)
        elements = [ui.TitleWithCross('validator', css_id)]
        elements += self._html_elements()
        return ui.CollapsibleColumn(elements, ui.ThumbnailField('validator', css_id), css_id).html
