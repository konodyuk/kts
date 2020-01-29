from typing import List

from kts.modelling.mixins import Model
from kts.util.misc import SourceMetaClass


class CustomModelSourceMetaClass(SourceMetaClass):
    def check_methods(methods):
        required_methods = ["get_tracked_params"]
        for meth in required_methods:
            assert (meth in methods), f"Method .{meth}() is required to define a custom model"


class CustomModel(Model, metaclass=CustomModelSourceMetaClass):
    def get_tracked_params(self):
        return []

    def preprocess(self, X, y=None):
        """Preprocess input before feeding it into model

        Args:
          X: np.array
          y: np.array or None (fitting or inference)

        Returns:
          X_processed, y_processed)

        """
        return X, y


def custom_model(ModelClass: type, tracked_params: List[str], name: str = None):
    raise NotImplemented
    if name is None:
        name = ModelClass.__name__
    return type(name, (ModelClass, CustomModel), {'get_tracked_params': lambda self: tracked_params})
