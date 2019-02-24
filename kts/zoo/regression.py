from ..modelling import *
from .. import config

from xgboost import XGBRegressor as _XGBR
class XGBRegressor(Model):
    Estimator = _XGBR
    default_params = {
        "max_depth": 3, 
        "learning_rate": 0.1, 
        "n_estimators": 100, 
        "booster": 'gbtree', 
        "gamma": 0, 
        "min_child_weight": 1, 
        "max_delta_step": 0, 
        "subsample": 1, 
        "colsample_bytree": 1, 
        "colsample_bylevel": 1, 
        "reg_alpha": 0, 
        "reg_lambda": 1, 
        "scale_pos_weight": 1, 
        "base_score": 0.5,
    }
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
        "n_jobs": 1, 
        "random_state": 0, 
        "seed": config.seed
    }
    short_name = 'xgb'
    
    
from lightgbm import LGBMRegressor as _LGBMR
class LGBMRegressor(Model):
    Estimator = _LGBMR
    short_name = 'lgb'
    
    
from catboost import CatBoostRegressor as _CBR
class CBRegressor(Model):
    Estimator = _CBR
    short_name = 'cb'
    