from typing import List

from kts.modelling.mixins import Model, NormalizeFillNAMixin
from kts.util.misc import SourceMetaClass


class CustomModelSourceMetaClass(SourceMetaClass):
    def check_methods(members):
        required_members = ["ignored_params"]
        for item in required_members:
            assert (item in members), f"Member {meth} is required to define a custom model"


class CustomModel(Model, metaclass=CustomModelSourceMetaClass):
    ignored_params = []

    def preprocess(self, X, y=None):
        """Preprocess input before feeding it into model

        Args:
          X: np.array
          y: np.array or None (fitting or inference)

        Returns:
          X_processed, y_processed)

        """
        return X, y


def custom_model(ModelClass: type, ignored_params: List[str], name: str = None, normalize_fillna: bool = False):
    if name is None:
        name = ModelClass.__name__
    bases = (ModelClass, CustomModel)
    if normalize_fillna:
        bases = (NormalizeFillNAMixin,) + bases
    return type(name, bases, {'ignored_params': ignored_params})
