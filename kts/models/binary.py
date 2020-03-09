from kts.modelling.mixins import BinaryClassifierMixin, NormalizeFillNAMixin
from kts.models.common import XGBMixin, LGBMMixin, CatBoostMixin, all_estimators, BLACKLISTED_PARAMS

__all__ = []


class XGBClassifier(BinaryClassifierMixin, XGBMixin): pass
class LGBMClassifier(BinaryClassifierMixin, LGBMMixin): pass
class CatBoostClassifier(BinaryClassifierMixin, CatBoostMixin): pass
__all__.extend(['XGBClassifier', 'LGBMClassifier', 'CatBoostClassifier'])


for name, estimator in all_estimators(type_filter='classifier'):
    if not hasattr(estimator, 'predict_proba'):
        continue
    globals()[name] = type(name,
                           (BinaryClassifierMixin, estimator, NormalizeFillNAMixin),
                           {'ignored_params': BLACKLISTED_PARAMS})
    __all__.append(name)

del name
del estimator
del all_estimators
