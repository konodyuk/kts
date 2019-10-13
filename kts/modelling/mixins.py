import numpy as np
from hashlib import sha256
import json


class NamingMixin:
    """ """
    @property
    def __name__(self):
        try:
            ctx_short_name = self.short_name
        except:
            ctx_short_name = self.get_short_name()
        try:
            ctx_tracked_params = self.tracked_params
        except:
            ctx_tracked_params = self.get_tracked_params()
        self.params = {
            key: self.get_params()[key]
            for key in ctx_tracked_params if key in self.get_params()
        }
        return f"{ctx_short_name}_{sha256((json.dumps(self.params, sort_keys=True)).encode()).hexdigest()[-3:] if self.params else 'default'}"


class ArithmeticMixin:
    """ """
    def __mul__(self, x):
        return WeightedModel(self, x)

    def __rmul__(self, x):
        return self * x

    def __truediv__(self, x):
        assert x != 0
        return WeightedModel(self, 1.0 / x)

    def __add__(self, other):
        # assert type(self).__base__ == type(other).__base__, \
        # f"Can't add {type(other).__name__} to {type(self).__name__}."
        if other == 0:
            return self
        return Ensemble([self, other])

    def __radd__(self, other):
        return self + other


class SourceMixin:
    """ """
    @property
    def source(self):
        """ """
        args = []
        for key, value in self.get_params().items():
            args.append(f"{key}={repr(value)}")
        res = ", ".join(args)
        return f"{self.__class__.__name__}({res})"


class PreprocessingMixin:
    """ """
    def preprocess(self, X, y):
        """Preprocess input before feeding it into model

        Args:
          X: np.array
          y: np.array or None (fitting or inference)

        Returns:
          X_processed, y_processed)

        """
        return X, y

    def preprocess_fit(self, X, y, *args, **kwargs):
        """

        Args:
          X:
          y:
          *args:
          **kwargs:

        Returns:

        """
        X_proc, y_proc = self.preprocess(X, y)
        self.fit(X_proc, y_proc, *args, **kwargs)

    def preprocess_predict(self, X, *args, **kwargs):
        """

        Args:
          X:
          *args:
          **kwargs:

        Returns:

        """
        X_proc, _ = self.preprocess(X, None)
        return self.predict(X_proc, *args, **kwargs)


class Model(ArithmeticMixin, NamingMixin, SourceMixin, PreprocessingMixin):
    """ """
    def __repr__(self):
        return f"[{self.__name__}] {self.source}"


class WeightedModel(ArithmeticMixin, PreprocessingMixin):  # MulNode
    """Multiplies predictions by a certain coefficient."""
    def __init__(self, model, coeff):
        self.model = model
        self.coeff = coeff
        if type(self.model) == type(self):
            self.coeff *= self.model.coeff
            self.model = self.model.model
        self.__name__ = f"{round(self.coeff, 2)} * "
        if "+" in self.model.__name__:
            self.__name__ += f"({self.model.__name__})"
        else:
            self.__name__ += f"{self.model.__name__}"

    def predict(self, X):
        """Standard prediction method.

        Args:
          X: data matrix

        Returns:

        """
        return self.coeff * self.model.preprocess_predict(X)


class Ensemble(ArithmeticMixin, PreprocessingMixin):  # AddNode
    """Sums up all predictions of all models."""
    def __init__(self, models):
        self.models = []
        ensembles = [model for model in models if type(model) == type(self)]
        single_models = [model for model in models if type(model) != type(self)]
        _models = []
        for ensemble in ensembles:
            _models += ensemble.models
        _models += single_models

        def __get_model_coeff(model):
            """
            :param model:
            :return: if the model is weighted, then returns its coefficient, else 1
            """
            if isinstance(model, WeightedModel):
                return model.model, model.coeff
            return model, 1

        self.models = list(
            set([__get_model_coeff(model)[0] for model in _models]))
        _coeffs = np.zeros(len(self.models))
        for i in range(len(self.models)):
            for model in _models:
                if __get_model_coeff(
                        self.models[i])[0] == __get_model_coeff(model)[0]:
                    _coeffs[i] += __get_model_coeff(model)[1]

        self.models = [
            WeightedModel(model, coeff)
            for model, coeff in zip(self.models, _coeffs)
        ]
        self.norm_coeff = _coeffs.sum()
        _coeffs /= self.norm_coeff
        self.__name__ = " + ".join([(model / self.norm_coeff).__name__
                                    for model in self.models])

    def predict(self, X):
        """Standard prediction method.

        Args:
          X: data matrix

        Returns:

        """
        res = 0
        for model in self.models:
            res += model.preprocess_predict(X)
        return res / self.norm_coeff

    def __mul__(self, x):
        return Ensemble([model * x for model in self.models])

    def __truediv__(self, x):
        assert x != 0
        return Ensemble([model / x for model in self.models])


class BinaryClassifierMixin(Model):
    """ """
    def predict(self, X, **kwargs):
        """

        Args:
          X:
          **kwargs:

        Returns:

        """
        return self.predict_proba(X, **kwargs)[:, 1]


class MultiClassifierMixin(Model):
    """ """
    def predict(self, X, **kwargs):
        """

        Args:
          X:
          **kwargs:

        Returns:

        """
        return self.predict_proba(X, **kwargs)


class RegressorMixin(Model):
    """ """

    pass
