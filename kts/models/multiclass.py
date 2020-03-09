from kts.modelling.mixins import MultiClassifierMixin, NormalizeFillNAMixin
from kts.zoo.common import XGBMixin, LGBMMixin, CatBoostMixin, all_estimators, BLACKLISTED_PARAMS

__all__ = []


class XGBClassifier(MultiClassifierMixin, XGBMixin): pass
class LGBMClassifier(MultiClassifierMixin, LGBMMixin): pass
class CatBoostClassifier(MultiClassifierMixin, CatBoostMixin): pass
__all__.extend(['XGBClassifier', 'LGBMClassifier', 'CatBoostClassifier'])


for name, estimator in all_estimators(type_filter='classifier'):
    if not hasattr(estimator, 'predict_proba'):
        continue
    globals()[name] = type(name,
                           (MultiClassifierMixin, estimator, NormalizeFillNAMixin),
                           {'ignored_params': BLACKLISTED_PARAMS})
    __all__.append(name)

del name
del estimator
del all_estimators
