from ..model import *
from .. import config

from xgboost import XGBClassifier as _XGBC
class XGBClassifier(Model):
    Estimator = _XGBC
    search_spaces = {
        'max_depth': (0, 50),
        'learning_rate': (0.01, 1.0, 'log-uniform'),
        'n_estimators': (50, 100),
        'booster': ['gbtree', 'dart'],
        'gamma': (1e-9, 0.5, 'log-uniform'),
        'min_child_weight': (0, 10),
        'max_delta_step': (0, 20),
        'subsample': (0.01, 1.0, 'uniform'),
        'colsample_bytree': (0.01, 1.0, 'uniform'),
        'colsample_bylevel': (0.01, 1.0, 'uniform'),
        'reg_alpha': (1e-9, 1.0, 'log-uniform'),
        'reg_lambda': (1e-9, 1000, 'log-uniform'),
        'scale_pos_weight': (1e-6, 500, 'log-uniform'),
        'base_score': (0.2, 0.7, 'uniform')
    }
    system_params = {
        "objective": 'binary:logistic', 
        "silent": True,
        "random_state": 0, 
        "seed": config.seed
    }
    short_name = 'xgb'

    def __init__(self, params=None, n_jobs=-1, verbosity=0):
        super().__init__(params, n_jobs, verbosity)
        self.estimator.n_jobs = n_jobs
        self.estimator.verbose = verbosity

    def _fit(self, X, y, **kwargs):
        self.estimator.fit(X, y, verbose=self.verbosity, **kwargs)

    def predict(self, X):
        return self.estimator.predict_proba(X)[:, 1]
    
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
