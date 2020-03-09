from kts.modelling.mixins import BinaryMixin, NormalizeFillNAMixin
from kts.models.common import XGBMixin, LGBMMixin, CatBoostMixin, all_estimators, BLACKLISTED_PARAMS

__all__ = []


class XGBClassifier(BinaryMixin, XGBMixin): pass
class LGBMClassifier(BinaryMixin, LGBMMixin): pass
class CatBoostClassifier(BinaryMixin, CatBoostMixin): pass
__all__.extend(['XGBClassifier', 'LGBMClassifier', 'CatBoostClassifier'])


for name, estimator in all_estimators(type_filter='classifier'):
    if not hasattr(estimator, 'predict_proba'):
        continue
    globals()[name] = type(name,
                           (BinaryMixin, estimator, NormalizeFillNAMixin),
                           {'ignored_params': BLACKLISTED_PARAMS})
    __all__.append(name)

del name
del estimator
del all_estimators
