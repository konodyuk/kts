from abc import ABC, abstractmethod
from typing import Dict, Optional

import pandas as pd

from kts.core.feature_set import Fold
from kts.modelling.mixins import Model


class ImportanceEstimator(ABC):
    @abstractmethod
    def estimate(self, fold: Fold, model: Model) -> Dict[str, float]:
        raise NotImplementedError


class Builtin(ImportanceEstimator):
    def estimate(self, fold: Fold, model: Model) -> Dict[str, float]:
        return dict(zip(fold.columns, model.feature_importances_))


class Permutation(ImportanceEstimator):
    def __init__(self, frame: pd.DataFrame, head: Optional[int]):
        pass


class SHAP(ImportanceEstimator):
    pass
