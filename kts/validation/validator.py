from copy import deepcopy

import numpy as np
import pandas as pd
from fastprogress import master_bar
from fastprogress.fastprogress import IN_NOTEBOOK

from kts.api.feature import feature_list
from kts.api.helper import helper_list
from kts.core.feature_set import FeatureSet
from kts.modelling.mixins import Ensemble, Model
from kts.modelling.pipeline import Pipeline
from kts.util.misc import SourceMetaClass
from kts.validation.experiment import Experiment
from kts.validation.leaderboard import leaderboard

if IN_NOTEBOOK:
    from IPython.display import HTML


class Validator(metaclass=SourceMetaClass):
    """ """
    def __init__(self, splitter, metric):
        self.splitter = splitter
        self.metric = metric

    def split(self, splitter, featureset):
        """

        Args:
          splitter: 
          featureset: 

        Returns:

        """
        y = featureset.target.values
        for idx_train, idx_test in splitter.split(y, y):
            yield idx_train, idx_test

    def update_oof(self, oof, weights, y_pred, idx_test, full_target):
        """

        Args:
          oof: 
          weights: 
          y_pred: 
          idx_test: 
          full_target: 

        Returns:

        """
        if oof is None or weights is None:
            oof = weights = np.zeros(shape=(full_target.shape[0], ) +
                                     y_pred.shape[1:],
                                     dtype=np.float)
        oof[idx_test] = (weights[idx_test] * oof[idx_test] +
                         y_pred) / (weights[idx_test] + 1)
        weights[idx_test] += 1
        return oof, weights

    def oof_to_df(self, oof, featureset):
        """

        Args:
          oof: 
          featureset: 

        Returns:

        """
        if len(oof.shape) == 1 or oof.shape[1] == 1:
            res = pd.DataFrame({"prediction": oof})
        else:
            res = pd.DataFrame(
                {f"prediction_{i}": oof[:, i]
                 for i in range(oof.shape[1])})
        res.set_index(featureset.target.index, inplace=True)
        return res

    def evaluate(self, y_true, y_pred):
        """

        Args:
          y_true: 
          y_pred: 

        Returns:

        """
        return self.metric(y_true, y_pred)

    def get_ensemble_name(self, model, featureset):
        """

        Args:
          model: 
          featureset: 

        Returns:

        """
        return f"{model.__name__}_x{self.splitter.get_n_splits()}-{featureset.__name__}"

    def __repr__(self):
        return f"Validator({self.splitter}, {self.metric.__name__})"

    def score(
            self,
            model: Model,
            featureset: FeatureSet,
            description: str = None,
            desc: str = None,
            **fit_params,
    ):
        """

        Args:
          model: 
          featureset:
          description:  (Default value = None)
          desc:  (Default value = None)
          **fit_params: 

        Returns:

        """
        if desc is not None and description is not None:
            raise ValueError(
                "desc is an alias of description. You can't use both")
        if desc is not None:
            description = desc
        pipelines = []
        scores = []
        oof = weights = None
        ensemble_name = self.get_ensemble_name(model, featureset)
        mb = master_bar(
            self.split(self.splitter, featureset),
            total=self.splitter.get_n_splits(),
            total_time=IN_NOTEBOOK,
        )
        mb.write(f"Validation of {ensemble_name}:")
        for idx_train, idx_test in mb:
            c_model = deepcopy(model)
            fsl = featureset.slice(idx_train, idx_test)
            pl = Pipeline(c_model, fsl)
            pl.fit(masterbar=mb, **fit_params)
            y_pred = pl.predict(idx_test, masterbar=mb)
            y_true = featureset.target.values[idx_test]
            pipelines.append(pl)
            scores.append(self.evaluate(y_true, y_pred))
            oof, weights = self.update_oof(oof, weights, y_pred, idx_test,
                                           featureset.target.values)
        final_ensemble = Ensemble(pipelines)
        final_ensemble = final_ensemble / len(pipelines)
        final_ensemble.__name__ = ensemble_name
        score = np.mean(scores)
        std = np.std(scores)
        oof = self.oof_to_df(oof, featureset)

        exp = Experiment(
            pipeline=final_ensemble,
            oof=oof,
            score=score,
            std=std,
            description=description,
            validator=self,
            feature_list=list(feature_list),
            helper_list=list(helper_list),
        )
        if IN_NOTEBOOK:
            mb.text = f"ID: {exp.identifier}<p> Score: {score}<p>" + mb.text
            mb.out.update(HTML(mb.text))
        leaderboard.register(exp)
        return {"score": score, "id": exp.identifier}
