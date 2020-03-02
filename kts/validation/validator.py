import numpy as np

import kts.ui.components as ui
from kts.core.feature_set import FeatureSet
from kts.modelling.mixins import Model
from kts.modelling.pipeline import CVPipeline
from kts.settings import cfg
from kts.ui.feature_computing_report import FeatureComputingReport
from kts.util.misc import SourceMetaClass
from kts.validation.experiment import Experiment, experiment_id
from kts.validation.leaderboard import leaderboard_list


class Validator(ui.HTMLRepr, metaclass=SourceMetaClass):
    def __init__(self, splitter, metric):
        self.splitter = splitter
        self.metric = metric

    def create_folds(self, feature_set, splitter):
        y = feature_set.target
        for idx_train, idx_test in splitter.split(y, y):
            yield idx_train, idx_test

    def create_oof(self, predictions, folds):
        max_idx = 0
        for idx_train, idx_val in folds:
            max_idx = max(max_idx, np.max(idx_train), np.max(idx_val))
        max_idx += 1
        res = np.zeros((max_idx,))
        weights = np.zeros((max_idx,))
        for pred, (idx_train, idx_val) in zip(predictions, folds):
            res[idx_val] += pred
            weights[idx_val] += 1
        return res / weights

    def evaluate(self, y_true, y_pred, fold_feature_set):
        return self.metric(y_true, y_pred)

    def score(self, model: Model, feature_set: FeatureSet, desc: str, leaderboard: str, **kwargs):
        cfg.preview_mode = False
        if leaderboard in leaderboard_list and experiment_id(model, feature_set) in leaderboard_list[leaderboard]:
            raise UserWarning(f'Duplicate experiment: {experiment_id(model, feature_set)}')
        folds = self.create_folds(feature_set, self.splitter)
        folds = list(folds)
        report = FeatureComputingReport()
        cv_feature_set = feature_set.split(folds)
        cv_feature_set.compute(report=report)
        cv_pipeline = CVPipeline(cv_feature_set, model)
        cv_pipeline.fit(score_fun=self.evaluate, **kwargs)
        raw_oof = cv_pipeline.raw_oof
        oof = self.create_oof(raw_oof, folds)
        cv_pipeline.raw_oof = None
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
