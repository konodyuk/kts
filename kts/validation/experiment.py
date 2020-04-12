import time
from copy import copy
from typing import Union, Optional

import numpy as np
import pandas as pd

import kts.ui.components as ui
from kts.core.feature_set import FeatureSet
from kts.feature_selection import Builtin
from kts.settings import cfg
from kts.ui.docstring import html_docstring
from kts.ui.feature_importances import FeatureImportances


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
        self.id = self.cv_pipeline.id
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

    @html_docstring
    def feature_importances(self, plot=True, estimator=Builtin(), sort_by='mean', n_best=None, verbose=None) -> Optional[Union[pd.DataFrame, FeatureImportances]]:
        """Computes feature importance

        Args:
            plot: if true, then returns a graph, otherwise returns a dataframe
            estimator: importance estimator instance used to compute feature importances
            sort_by: fold-wise statistic used to sort features. One of min, mean, and max
            n_best: number of best features to show
            verbose: whether to produce reports during computing, such as progress bar
                and interim feature importances. Useful for long-running estimators

        Returns:
            A feature importances graph if plot=True, dataframe with importances otherwise

        Examples:
            >>> from kts.feature_selection import Permutation
            >>> lb.ABCDEF.feature_importances(plot=False)  # -> pd.DataFrame
            >>> lb.ABCDEF.feature_importances(estimator=Permutation(train_frame, n_iters=3), sort_by='max')
        """
        estimator.sort_by = sort_by
        estimator.n_best = n_best
        if verbose is not None:
            estimator.verbose = verbose
        estimator.process(self)
        if plot:
            if estimator.verbose:
                return None
            else:
                return estimator.report
        return estimator.result

    @html_docstring
    def select(self, n_best, estimator=Builtin(), sort_by='max', verbose=None) -> FeatureSet:
        """Computes feature importances and returns a FeatureSet containing best features

        Args:
            n_best: number of best features to select
            estimator: importance estimator instance used to compute feature importances
            sort_by: fold-wise statistic used to sort features. One of min, mean, and max
            verbose: whether to produce reports during computing, such as progress bar
                and interim feature importances. Useful for long-running estimators

        Returns:
            A FeatureSet instance containing only selected features.
            Feature constructors containing no features are dropped.

        Examples:
            >>> from kts.feature_selection import Permutation
            >>> fs_sel = lb.ABCDEF.select(30, estimator=Permutation(train_frame, n_iters=3, sample=400))
            >>> val.score(fs_sel, CatBoostClassifier())  # new feature set can be used right after selection
        """
        estimator.sort_by = sort_by
        estimator.n_best = n_best
        if verbose is not None:
            estimator.verbose = verbose
        estimator.process(self)
        best_columns = [i['name'] for i in estimator.to_list()]
        best_columns = best_columns[:n_best]
        new_feature_set = copy(self.feature_set)
        new_before_split = [i & best_columns for i in new_feature_set.before_split]
        new_after_split = [i & best_columns for i in new_feature_set.after_split]
        new_before_split = [i for i in new_before_split if i.columns]
        new_after_split = [i for i in new_after_split if i.columns]
        new_feature_set.before_split = new_before_split
        new_feature_set.after_split = new_after_split
        return new_feature_set

    def move_to(self, lb_name: str):
        raise NotImplemented

    @property
    def alias(self):
        return ExperimentAlias(self)

    @property
    def _html_elements(self):
        elements = [
            ui.Annotation('ID'), ui.Field(self.id),
            ui.Annotation('score'), ui.Field(self.score),
        ]
        if self.description:
            elements += [ui.Annotation('description'), ui.Field(self.description)]
        elements += [
            ui.Annotation('model'), ui.Field(self.model_class),
            ui.Annotation('params'), ui.Code(self.model.format_params(prettify=True)),
            ui.Annotation('features'), self.feature_set.feature_pool,
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

    def set_train_frame(self, train_frame: pd.DataFrame):
        self.cv_pipeline.set_train_frame(train_frame)
        cfg.preview_mode = False
        folds = self.validator.create_folds(self.feature_set, self.validator.splitter)
        folds = list(folds)
        cfg.preview_mode = True
        self.cv_pipeline.set_folds(folds)


class ExperimentAlias(ui.HTMLRepr):
    def __init__(self, experiment):
        self._html_elements = experiment._html_elements
        self.id = experiment.id
        self.score = experiment.score
        self.model_class = experiment.model_class
        self.n_features = experiment.cv_pipeline.cv_feature_set.n_features
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
