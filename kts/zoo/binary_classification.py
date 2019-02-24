from ..modelling import *
from .. import config

from xgboost import XGBClassifier as _XGBC
class XGBClassifier(Model):
    Estimator = _XGBC
    tracked_params = ['n_estimators', 'max_depth']
    fit_params = {'verbose': False}
    short_name = 'xgb'
    
from lightgbm import LGBMClassifier as _LGBMC
class LGBMClassifier(Model):
    Estimator = _LGBMC
    search_spaces = {
        'num_leaves': (2, 50),
        'learning_rate': (0.01, 1.0),
        'max_depth': (0, 50),
        'min_data_in_leaf': (20, 200),
        'max_bin': (2, 255),
    }

    system_params = {
        "metric": 'binary_logloss',
        "verbosity": 1,
        "seed": config.seed}
    short_name = 'lgb'
    
    def predict(self, X):
        return self.estimator.predict_proba(X)[:, 1]
    
from catboost import CatBoostClassifier as _CBC
class CBClassifier(Model):
    Estimator = _CBC
    short_name = 'cb'
    def __init__(self, params=None, n_jobs=-1, verbosity=0):
        super().__init__(params, n_jobs, verbosity)
        self.verbose = verbosity
        self.estimator.thread_count = n_jobs
        # self.estimator.verbose = verbosity

    def _fit(self, X, y, **kwargs):
        self.estimator.fit(X, y, verbose=self.verbose)

    def predict(self, X):
        return self.estimator.predict_proba(X)[:, 1]

from sklearn.linear_model import LogisticRegression as _LOGR
class LogisticRegression(Model):
    Estimator = _LOGR
    short_name = 'logr'
    search_spaces = {
        'C': [10**x for x in range(-4, 5)],
        'penalty': ['l1', 'l2'],
        'fit_intercept': [True, False],
        'class_weight': [None, 'balanced']
    }
    # max_iter and warm_start are useless for liblinear solver
    system_params = {
        'tol': 0.0001,
        'solver': 'liblinear',
        'multi_class': 'auto',
        'n_jobs': -1,
        'verbose': 0,
        'random_state': 42
    }

    def predict(self, X):
        return self.estimator.predict_proba(X)[:, 1]
