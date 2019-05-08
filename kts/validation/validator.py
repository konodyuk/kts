import numpy as np

from .experiment import experiment_list, Experiment
from .leaderboard import leaderboard
from ..pipeline import Pipeline
from ..modelling import Ensemble
from copy import deepcopy
import tqdm


class Validator:
    def __init__(self, splitter, metric, enable_widget=False):
        self.splitter = splitter
        self.metric = metric
        self.bar = (tqdm.tqdm_notebook if enable_widget else tqdm.tqdm)
        
    def score(self, model, featureset, description=None, desc=None, **fit_params):
        try:
            _ = model.__name__
        except:
            raise AttributeError("Model must have .__name__ attribute to be validated")
        try:
            _ = model.source
        except:
            raise AttributeError("Model must have .source attribute to be validated")
        if desc is not None and description is not None:
            raise ValueError("desc is an alias of description. You can't use both")
        if desc is not None:
            description = desc
        pipelines = []
        scores = []
        y = featureset.target
        oofs = np.zeros_like(y, dtype=np.float)
        weights = np.zeros_like(y, dtype=np.float)
        model_name = f"{model.__name__}_x{self.splitter.get_n_splits()}-{featureset.__name__}"
        pbar = self.bar(self.splitter.split(y, y), total=self.splitter.get_n_splits())
        pbar.set_description_str(f"Val of {model_name}")
        for idx_train, idx_test in pbar:
            c_model = deepcopy(model)
            fsl = featureset.slice(idx_train)
            pl = Pipeline(c_model, fsl)

            fsl()
            try:
                pl.fit(eval_set=[(fsl(idx_test).values, featureset.target[idx_test].values)], **fit_params)
            except:
                pl.fit(**fit_params)

            pred = pl.predict(idx_test)
            pl.featureslice.compress()
            oofs[idx_test] = (weights[idx_test] * oofs[idx_test] + pred) / (weights[idx_test] + 1)
            weights[idx_test] += 1
            # print(featureset.target[idx_test].values[:10], pred[:10])
            score = self.metric(featureset.target.values[idx_test], pred)
            pipelines.append(pl)
            scores.append(score)
            pbar.set_postfix_str(f"score: {np.mean(scores)}")
        final_ensemble = Ensemble(pipelines)
        final_ensemble = final_ensemble / len(pipelines)
        final_ensemble.__name__ = model_name
        score = np.mean(scores)
        std = np.std(scores)
        exp = Experiment(
                pipeline=final_ensemble,
                oofs=oofs,
                score=score,
                std=std,
                description=description,
                splitter=self.splitter,
                metric=self.metric)
        # experiment_list.register(exp)
        leaderboard.register(exp)
        return score

