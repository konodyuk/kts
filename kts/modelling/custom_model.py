from kts.modelling.mixins import Model
from kts.util.misc import SourceMetaClass


class CustomModelSourceMetaClass(SourceMetaClass):
    """ """
    def check_methods(methods):
        """

        Args:
          methods:

        Returns:

        """
        required_methods = ["get_short_name", "get_tracked_params"]
        for meth in required_methods:
            assert (meth in methods), f"Method .{meth}() is required to define a custom model"


class CustomModel(Model, metaclass=CustomModelSourceMetaClass):
    """ """
    def get_short_name(self):
        """ """
        return "custom_model"

    def get_tracked_params(self):
        """ """
        return []

    def preprocess(self, X, y):
        """Preprocess input before feeding it into model

        Args:
          X: np.array
          y: np.array or None (fitting or inference)

        Returns:
          X_processed, y_processed)

        """
        return X, y