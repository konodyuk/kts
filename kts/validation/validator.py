import numpy as np

from .. import config
from .experiment import experiment_list, Experiment
from ..pipeline import Pipeline
# from ..feature.storage import FeatureSlice
from copy import deepcopy
import tqdm

class Validator:
    def __init__(self, splitter, metric, enable_widget=False):
        self.splitter = splitter
        self.metric = metric
        self.bar = (tqdm.tqdm_notebook if enable_widget else tqdm.tqdm)
        
    def score(self, model, featureset, **fit_params):
        pipelines = []
        scores = []
        oofs = np.zeros_like(featureset.target, dtype=np.float)
        weights = np.zeros_like(featureset.target, dtype=np.float)
        pbar = self.bar(self.splitter.split, total=self.splitter.size)
        pbar.set_description_str(f"Val of {model.__name__}")
        for spl in pbar:
            idx_train = spl['train']
            idx_test = spl['test']
            c_model = deepcopy(model)
            fsl = featureset.slice(idx_train)
            pl = Pipeline(c_model, fsl)
            try:
                pl.fit(eval_set=[(fsl(idx_test).values, featureset.target[idx_train].values)], **fit_params)
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
        model_name = f"ens_{pipelines[0].model.__name__}_x{len(pipelines)}"
        final_ensemble = pipelines[0]
        for pipeline in pipelines[1:]:
            final_ensemble = final_ensemble + pipeline
        final_ensemble = final_ensemble / len(pipelines)
        final_ensemble.__name__ = model_name
        score = np.mean(scores)
        std = np.std(scores)
        experiment_list.register(
            Experiment(
                final_ensemble,
                oofs,
                score,
                std
            )
        )
        return score