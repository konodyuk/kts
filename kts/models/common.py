from kts.modelling.mixins import ProgressMixin, Placeholder


try:
    from xgboost import XGBClassifier as _XGBClassifier
    from xgboost import XGBRegressor as _XGBRegressor
except ImportError:
    _XGBClassifier = Placeholder
    _XGBRegressor = Placeholder
try:
    from lightgbm import LGBMClassifier as _LGBMClassifier
    from lightgbm import LGBMRegressor as _LGBMRegressor
except ImportError:
    _LGBMClassifier = Placeholder
    _LGBMRegressor = Placeholder
try:
    from catboost import CatBoostClassifier as _CatBoostClassifier
    from catboost import CatBoostRegressor as _CatBoostRegressor
except ImportError:
    _CatBoostClassifier = Placeholder
    _CatBoostRegressor = Placeholder


all_estimators = lambda **kw: []
try:
    from sklearn.utils import all_estimators
except ImportError:
    pass
try:
    from sklearn.utils.testing import all_estimators
except ImportError:
    pass


BLACKLISTED_PARAMS = [
    "verbose",
    "verbosity",
    "silent",
    "n_jobs",
    "n_threads",
    "threads",
    "nthread",
    "thread_count"
]


class XGBMixin(_XGBClassifier, ProgressMixin):
    ignored_params = BLACKLISTED_PARAMS

    def enable_verbosity(self):
        self.set_params(silent=False)

    def progress_callback(self, line):
        s = line
        pos = s.find(']')
        if pos != -1:
            step = s[1:pos]
            if not step.isdigit():
                return {'step': 0, 'train_score': 0, 'valid_score': 0, 'success': False}
        else:
            return{'step': 0, 'train_score': 0, 'valid_score': 0, 'success': False}
        s = s.split(':')
        valid_score = s[1]
        return {'step': step, 'train_score': None, 'valid_score': valid_score, 'success': True}

    def get_n_steps(self):
        return self.get_params()['n_estimators']


class LGBMMixin(_LGBMClassifier, ProgressMixin):
    ignored_params = BLACKLISTED_PARAMS

    def enable_verbosity(self):
        self.set_params(silent=False)

    def progress_callback(self, line):
        s = line
        pos = s.find(']')
        if pos != -1:
            step = s[1:pos]
            if not step.isdigit():
                return {'step': 0, 'train_score': 0, 'valid_score': 0, 'success': False}
        else:
            return {'step': 0, 'train_score': 0, 'valid_score': 0, 'success': False}
        s = s.split(':')
        valid_score = s[1].strip()
        return {'step': step, 'train_score': None, 'valid_score': valid_score, 'success': True}

    def get_n_steps(self):
        return self.get_params()['n_estimators']


class CatBoostMixin(_CatBoostClassifier, ProgressMixin):
    ignored_params = BLACKLISTED_PARAMS

    def enable_verbosity(self):
        self.set_params(verbose=True)

    def progress_callback(self, line):
        s = line
        pos = s.find(':')
        if pos != -1:
            step = s[:pos]
            if not step.isdigit():
                return {'step': 0, 'train_score': 0, 'valid_score': 0, 'success': False}
        else:
            return {'step': 0, 'train_score': 0, 'valid_score': 0, 'success': False}
        s = s.split()
        if 'learn' in s[1]:
            train_score = s[2]
            valid_score = s[4]
        else:
            valid_score = s[2]
            train_score = None
        return {'step': step, 'train_score': train_score, 'valid_score': valid_score, 'success': True}

    def get_n_steps(self):
        result = self.get_param('iterations')
        if result is None:
            result = self.get_param('n_estimators')
        if result is None:
            result = self.get_param('num_trees')
        if result is None:
            result = self.get_param('num_boost_round')
        if result is None:
            result = 1000
        return result
