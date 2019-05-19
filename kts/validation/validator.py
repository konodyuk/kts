import numpy as np

from .experiment import experiment_list, Experiment
from .leaderboard import leaderboard
from ..pipeline import Pipeline
from ..modelling import Ensemble
from copy import deepcopy
from fastprogress import master_bar, progress_bar
from fastprogress.fastprogress import IN_NOTEBOOK

if IN_NOTEBOOK:
    from IPython.display import HTML


class Validator:
    def __init__(self, splitter, metric):
        self.splitter = splitter
        self.metric = metric
        
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
        mb = master_bar(self.splitter.split(y, y),
                        total=self.splitter.get_n_splits(),
                        total_time=True)
        mb.write(f"Validation of {model_name}:")
        for idx_train, idx_test in mb:
            c_model = deepcopy(model)
            fsl = featureset.slice(idx_train)
            pl = Pipeline(c_model, fsl)
            pb = progress_bar(range(2), parent=mb)
            mb.child.comment = f'training...'
            pb.on_iter_begin()
            pb.update(0)
            fsl()
            try:
                pl.fit(eval_set=[(fsl(idx_test).values, featureset.target.values[idx_test])], **fit_params)
            except:
                mb.child.comment = 'failed to train with eval_set, training without it...'
                pl.fit(**fit_params)
            pb.update(1)
            mb.child.comment = 'validating...'
            pred = pl.predict(idx_test)
            pl.featureslice.compress()
            oofs[idx_test] = (weights[idx_test] * oofs[idx_test] + pred) / (weights[idx_test] + 1)
            weights[idx_test] += 1
            # print(featureset.target[idx_test].values[:10], pred[:10])
            score = self.metric(featureset.target.values[idx_test], pred)
            pipelines.append(pl)
            scores.append(score)
            pb.update(2)
            mb.first_bar.comment = f"{round(np.mean(scores), 7)}"
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
        if IN_NOTEBOOK:
            mb.text = f"ID: {exp.identifier}<p>" + mb.text
            mb.out.update(HTML(mb.text))
        leaderboard.register(exp)
        return score

