from ..model import *
from .. import config
from xgboost import XGBClassifier as _XGBC
from sklearn.neighbors import KNeighborsClassifier as _KNC


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
        "n_jobs": -1,
        "random_state": 0, 
        "seed": config.seed
    }
    short_name = 'xgb'


class KNeighborsClassifier(Model):
    Estimator = _KNC
    system_params = {
        "n_jobs": -1,
    }
    short_name = 'knc'

    def predict(self, X):
        return self.estimator.predict_proba(X)

