from kts.core.feature_constructor.base import BaseFeatureConstructor
from kts.core.frame import KTSFrame


class InlineFeatureConstructor(BaseFeatureConstructor):
    parallel = False
    cache = False

    def __call__(self, kf: KTSFrame, ret=True):
        return self.compute(kf, ret=ret)
