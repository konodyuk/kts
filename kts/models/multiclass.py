from kts.modelling.mixins import MulticlassMixin, NormalizeFillNAMixin
from kts.models.common import XGBMixin, LGBMMixin, CatBoostMixin, all_estimators, BLACKLISTED_PARAMS

__all__ = []


class XGBClassifier(MulticlassMixin, XGBMixin): pass
class LGBMClassifier(MulticlassMixin, LGBMMixin): pass
class CatBoostClassifier(MulticlassMixin, CatBoostMixin): pass
__all__.extend(['XGBClassifier', 'LGBMClassifier', 'CatBoostClassifier'])


for name, estimator in all_estimators(type_filter='classifier'):
    if not hasattr(estimator, 'predict_proba'):
        continue
    globals()[name] = type(name,
                           (MulticlassMixin, estimator, NormalizeFillNAMixin),
                           {'ignored_params': BLACKLISTED_PARAMS})
    __all__.append(name)

del name
del estimator
del all_estimators
