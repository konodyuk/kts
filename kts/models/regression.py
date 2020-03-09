from kts.modelling.mixins import RegressorMixin, NormalizeFillNAMixin
from kts.models.common import XGBMixin, LGBMMixin, CatBoostMixin, all_estimators, BLACKLISTED_PARAMS

__all__ = []


class XGBRegressor(RegressorMixin, XGBMixin): pass
class LGBMRegressor(RegressorMixin, LGBMMixin): pass
class CatBoostRegressor(RegressorMixin, CatBoostMixin): pass
__all__.extend(['XGBRegressor', 'LGBMRegressor', 'CatBoostRegressor'])


for name, estimator in all_estimators(type_filter='regressor'):
    globals()[name] = type(name,
                           (RegressorMixin, estimator, NormalizeFillNAMixin),
                           {'ignored_params': BLACKLISTED_PARAMS})
    __all__.append(name)

del name
del estimator
del all_estimators
