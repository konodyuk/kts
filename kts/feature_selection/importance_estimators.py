import time
from abc import ABC
from contextlib import redirect_stdout

import numpy as np
import pandas as pd
from IPython.display import display

from kts.settings import cfg
from kts.ui.feature_importances import FeatureImportances, ImportanceComputingReport


class ImportanceEstimator(ABC):
    verbose = False
    handle = None
    last_update = 0
    update_interval = 0.1
    sort_by = 'mean'
    n_best = None

    def setup(self, experiment):
        pass

    def setup_fold(self, fold, model):
        pass

    def step(self, i):
        pass

    def exit_fold(self):
        pass

    def exit(self):
        pass

    def to_list(self):
        return list(self.result
                    .agg(['name', 'min', 'max', 'mean'])
                    .sort_values(axis=1, by=self.sort_by, ascending=False)
                    .to_dict()
                    .values())

    @property
    def report(self):
        if self.result.empty:
            return
        payload = self.to_list()
        if self.n_best is not None:
            payload = payload[:self.n_best]
        return FeatureImportances(payload)

    def show(self):
        return display(self.report, display_id=True)

    def refresh(self, force=False):
        if not self.verbose:
            return
        if self.handle is None:
            with redirect_stdout(cfg.stdout):
                self.handle = self.show()
        if self.handle is None:
            # not in ipython
            return
        if force or time.time() - self.last_update >= self.update_interval:
            self.update()
            with redirect_stdout(cfg.stdout):
                self.handle.update(self.report)
            self.last_update = time.time()

    def update(self):
        all_folds = set(i['fold'] for i in self.steps)
        all_names = set(i['name'] for i in self.steps)
        self.result = pd.DataFrame(index=all_folds, columns=all_names)
        for step in self.steps:
            self.result.loc[step['fold'], step['name']] = step['value']

    def process(self, experiment):
        self.setup(experiment)
        cv_pipeline = experiment.cv_pipeline
        cv_feature_set = cv_pipeline.cv_feature_set
        n_folds = cv_feature_set.n_folds
        total = sum(len(cv_feature_set.fold(i).columns) for i in range(n_folds))
        pbar = ImportanceComputingReport(total)
        self.pbar = pbar
        self.steps = list()
        self.result = pd.DataFrame()
        for fold_idx in range(n_folds):
            fold = cv_feature_set.fold(fold_idx)
            model = cv_pipeline.models[fold_idx]
            columns = fold.columns
            self.setup_fold(fold, model)
            for feature_idx in range(len(columns)):
                name = columns[feature_idx]
                if self.verbose:
                    pbar.update(name)
                self.steps.append({'fold': fold_idx, 'name': name, 'value': self.step(feature_idx)})
                self.refresh()
            self.exit_fold()
        if self.verbose:
            pbar.refresh(force=True)
        self.update()
        self.refresh(force=True)
        self.exit()


class Builtin(ImportanceEstimator):
    verbose = False

    def __init__(self, sort_by='mean', n_best=None):
        self.sort_by = sort_by
        self.n_best = n_best

    def setup_fold(self, fold, model):
        self.model = model

    def step(self, i):
        return self.model.feature_importances_[i]

    def exit_fold(self):
        self.model = None


class Permutation(ImportanceEstimator):
    verbose = True

    def __init__(self, train_frame, n_iters=5, sample=None, sort_by='mean', random_state=None, n_best=None, verbose=True):
        self.train_frame = train_frame
        self.n_iters = n_iters
        self.sample = sample
        self.sort_by = sort_by
        self.rng = np.random.RandomState(random_state)
        self.n_best = n_best
        self.verbose = verbose

    def setup(self, experiment):
        experiment.set_train_frame(self.train_frame)
        self.metric = experiment.validator.metric

    def setup_fold(self, fold, model):
        cfg.preview_mode = False
        self.X = fold.valid
        self.y = fold.valid_target
        if self.sample is not None:
            self.X = self.X[:self.sample]
            self.y = self.y[:self.sample]
        cfg.preview_mode = True
        self.model = model
        self.base_score = self.metric(self.y, self.model.preprocess_predict(self.X))

    def step(self, i):
        unshuffled = self.X[:, i].copy()
        result = []
        for _ in range(self.n_iters):
            self.rng.shuffle(self.X[:, i])
            result.append(-self.metric(self.y, self.model.preprocess_predict(self.X)) + self.base_score)
        self.X[:, i] = unshuffled
        return np.mean(result)

    def exit_fold(self):
        self.X = None
        self.y = None
        self.model = None
        self.base_scores = None


class PermutationBlind(ImportanceEstimator):
    verbose = True

    def __init__(self, frame, n_iters=5, sample=None, sort_by='mean', random_state=None, n_best=None, verbose=True):
        self.frame = frame
        self.n_iters = n_iters
        self.sample = sample
        self.sort_by = sort_by
        self.rng = np.random.RandomState(random_state)
        self.verbose = verbose
        self.n_best = n_best

    def setup(self, experiment):
        self.metric = experiment.validator.metric

    def setup_fold(self, fold, model):
        cfg.preview_mode = False
        self.X = fold(self.frame)
        if self.sample is not None:
            self.X = self.X[:self.sample]
        cfg.preview_mode = True
        self.model = model
        self.base_pred = self.model.preprocess_predict(self.X)

    def step(self, i):
        unshuffled = self.X[:, i].copy()
        result = []
        for _ in range(self.n_iters):
            self.rng.shuffle(self.X[:, i])
            result.append(np.std(self.model.preprocess_predict(self.X) - self.base_pred))
        self.X[:, i] = unshuffled
        return np.mean(result)

    def exit_fold(self):
        self.X = None
        self.model = None
        self.base_pred = None
