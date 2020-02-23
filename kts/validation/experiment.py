import time

import numpy as np

import kts.ui.components
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
            kts.ui.components.Annotation('ID'),
            kts.ui.components.Field(self.id),
            kts.ui.components.Annotation('score'),
            kts.ui.components.Field(self.score),
            kts.ui.components.Annotation('description'),
            kts.ui.components.Field(self.description),
            kts.ui.components.Annotation('model'),
            kts.ui.components.Field(self.model.__class__),
            kts.ui.components.Annotation('params'),
            kts.ui.components.Field(self.model.get_params()),
            kts.ui.components.Annotation('features'),
            self.feature_set.feature_pool,
            kts.ui.components.Annotation('details'),
            kts.ui.components.Pool([
                kts.ui.components.Raw(self.feature_set.html_collapsible),
                kts.ui.components.Raw(self.model.html_collapsible),
                kts.ui.components.Raw(self.validator.html_collapsible)
            ])
        ]
        return elements
    
    @property
    def html(self):
        return kts.ui.components.Column([kts.ui.components.Title('experiment')] + self._html_elements).html
    
    def html_collapsible(self, thumbnail=None, border=False):
        cssid = np.random.randint(1000000000)
        if thumbnail is None:
            thumbnail = kts.ui.components.ThumbnailField(f"{self.id}", cssid)
        else:
            thumbnail.cssid = cssid
        elements = [kts.ui.components.TitleWithCross('experiment', cssid)]
        elements += self._html_elements
        return kts.ui.components.CollapsibleColumn(elements, thumbnail, cssid, outer=True, border=border).html
    


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
        return kts.ui.components.Column([kts.ui.components.Title('experiment')] + self._html_elements).html
    
    def html_collapsible(self, thumbnail=None, border=False):
        cssid = np.random.randint(1000000000)
        if thumbnail is None:
            thumbnail = kts.ui.components.ThumbnailField(f"{self.id}", cssid)
        else:
            thumbnail.cssid = cssid
        elements = [kts.ui.components.TitleWithCross('experiment', cssid)]
        elements += self._html_elements
        return kts.ui.components.CollapsibleColumn(elements, thumbnail, cssid, outer=True, border=border).html
