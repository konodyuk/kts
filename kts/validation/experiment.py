import time

import numpy as np

from kts.core import ui
from kts.util.hashing import hash_str


def experiment_id(model, feature_set):
    return hash_str(model.name + feature_set.name)


class Experiment:
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

    @property
    def model(self):
        return self.cv_pipeline.model

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

    def feature_importances(self, plot=True):
        raise NotImplemented

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
            ui.Field(self.model.__class__),
            ui.Annotation('params'),
            ui.Field(self.model.get_params()),
            ui.Annotation('features'),
            self.feature_set.feature_pool,
            ui.Annotation('details'),
            ui.Pool([
                ui.Raw(self.feature_set.html_collapsible),
                ui.Raw(self.model.html_collapsible),
                ui.Raw(self.validator.html_collapsible)
            ])
        ]
        return elements
    
    @property
    def html(self):
        return ui.Column([ui.Title('experiment')] + self._html_elements).html
    
    def html_collapsible(self, thumbnail=None, border=False):
        cssid = np.random.randint(1000000000)
        if thumbnail is None:
            thumbnail = ui.ThumbnailField(f"{self.id}", cssid)
        else:
            thumbnail.cssid = cssid
        elements = [ui.TitleWithCross('experiment', cssid)]
        elements += self._html_elements
        return ui.CollapsibleColumn(elements, thumbnail, cssid, outer=True, border=border).html
    


class ExperimentAlias:
    def __init__(self, experiment):
        self._html_elements = experiment._html_elements
        self.id = experiment.id
        self.score = experiment.score
        self.n_features = len(experiment.feature_set.before_split) + len(experiment.feature_set.before_split)
        self.date = experiment.date
        self.took = experiment.took

    @property
    def html(self):
        return ui.Column([ui.Title('experiment')] + self._html_elements).html
    
    def html_collapsible(self, thumbnail=None, border=False):
        cssid = np.random.randint(1000000000)
        if thumbnail is None:
            thumbnail = ui.ThumbnailField(f"{self.id}", cssid)
        else:
            thumbnail.cssid = cssid
        elements = [ui.TitleWithCross('experiment', cssid)]
        elements += self._html_elements
        return ui.CollapsibleColumn(elements, thumbnail, cssid, outer=True, border=border).html
