import sys
import time
from contextlib import redirect_stdout
from copy import deepcopy
from functools import partial

import numpy as np
from IPython.display import display

from kts.core.ui import *

avg = partial(np.mean, axis=0)

class ProgressParser:
    def __init__(self, handle, parser, report, min_interval=0.2):
        self.handle = handle
        self.parser = parser
        self.report = report
        self.min_interval = min_interval
        self.last_update = 0
        self.buf = ""
        
    def write(self, b):
        self.buf += b
        if self.buf.find('\n') != -1:
            self.flush()
        
    def flush(self, force=False):
        for line in self.buf.split('\n'):
            res = self.parser(line)
            if res['success']:
                train_score = float(res['train_score']) if res['train_score'] is not None else None
                valid_score = float(res['valid_score']) if res['valid_score'] is not None else None
                self.report.update(step=int(res['step']), 
                                   train_score=train_score, 
                                   valid_score=valid_score, 
                                   timestamp=time.time())
        self.buf = ""
        cur_time = time.time()
        if force or cur_time - self.last_update >= self.min_interval:
            with redirect_stdout(cfg.builtin_stdout):
                self.handle.update(self.report)
            self.last_update = cur_time

class CVPipeline:
    def __init__(self, cv_feature_set, model):
        self.cv_feature_set = cv_feature_set
        self.models = [deepcopy(model) for i in range(self.n_folds)]
        self.blend = avg
        self.scores = []
        self.raw_oof = []
        self.took = None

    @property
    def n_folds(self):
        return self.cv_feature_set.n_folds

    @property
    def model(self):
        return self.models[0]

    @property
    def feature_set(self):
        return self.cv_feature_set.feature_set
    
    def predict(self, frame):
        self.cv_feature_set.compute(frame)
        # ifr = InferenceReport(self.n_folds)  # TODO
        predictions = []
        for i in range(self.n_folds):
            # ifr.set_fold(i)
            model = self.models[i]
            fold = self.cv_feature_set.fold(i)
            x = fold(frame)
            y_pred = model.preprocess_predict(x)
            predictions.append(y_pred)
            # ifr.finish()
        return self.blend(predictions) 

    def fit(self, score_fun, **kwargs):
        cvr = CVFittingReport(self.n_folds, n_steps=self.model.get_n_steps())
        handle = display(cvr, display_id=True)
        start = time.time()
        for i in range(self.n_folds):
            cvr.set_fold(i)
            model = self.models[i]
            fold = self.cv_feature_set.fold(i)
            x_train = fold.train
            y_train = fold.train_target
            x_valid = fold.valid
            y_valid = fold.valid_target
            model.enable_verbosity()
            with redirect_stdout(ProgressParser(handle, self.model.progress_callback, cvr)):
                try:
                    model.preprocess_fit(x_train, y_train, eval_set=[(x_valid, y_valid)], **kwargs)
                except:
                    model.preprocess_fit(x_train, y_train, **kwargs)

                y_pred = model.preprocess_predict(x_valid)
                self.raw_oof.append(y_pred)
                score = score_fun(y_valid, y_pred, fold)
                self.scores.append(score)

                cvr.set_metric(score)
                sys.stdout.flush(force=True)
        self.took = time.time() - start
