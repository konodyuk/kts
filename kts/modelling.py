import numpy as np
from hashlib import sha256
import json


class NamingMixin:
    @property
    def __name__(self):
        self.params = {key: self.get_params()[key] for key in self.tracked_params if
                       key in self.get_params()}
        return f"{self.short_name}_{sha256((json.dumps(self.params, sort_keys=True)).encode()).hexdigest()[-3:] if self.params else 'default'}"


class ArithmeticMixin:
    def __mul__(self, x):
        return WeightedModel(self, x)

    def __rmul__(self, x):
        return self * x

    def __truediv__(self, x):
        assert x != 0
        return WeightedModel(self, 1. / x)

    def __add__(self, other):
        # assert type(self).__base__ == type(other).__base__, \
        # f"Can't add {type(other).__name__} to {type(self).__name__}."
        if other == 0:
            return self
        return Ensemble([self, other])

    def __radd__(self, other):
        return self + other


class SourceMixin:
    @property
    def source(self):
        args = []
        for key, value in self.get_params().items():
            args.append(f"{key}={repr(value)}")
        res = ', '.join(args)
        return f"{self.__class__.__name__}({res})"


class Model(ArithmeticMixin, NamingMixin, SourceMixin):
    tracked_params = []
    short_name = 'model'

    def __repr__(self):
        return self.__name__

class WeightedModel(ArithmeticMixin): # MulNode
    """
    Multiplies predictions by a certain coefficient.
    """
    def __init__(self, model, coeff):
        self.model = model
        self.coeff = coeff
        if type(self.model) == type(self):
            self.coeff *= self.model.coeff
            self.model = self.model.model
        self.__name__ = f"{round(self.coeff, 2)} * "
        if '+' in self.model.__name__:
            self.__name__ += f"({self.model.__name__})"
        else:
            self.__name__ += f"{self.model.__name__}"

    def predict(self, X):
        """
        Standard prediction method.
        :param X: data matrix
        :return:
        """
        return self.coeff * self.model.predict(X)


class Ensemble(ArithmeticMixin): # AddNode
    """
    Sums up all predictions of all models.
    """
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

        self.models = list(set([__get_model_coeff(model)[0] for model in _models]))
        _coeffs = np.zeros(len(self.models))
        for i in range(len(self.models)):
            for model in _models:
                if __get_model_coeff(self.models[i])[0] == __get_model_coeff(model)[0]:
                    _coeffs[i] += __get_model_coeff(model)[1]

        self.models = [WeightedModel(model, coeff) for model, coeff in zip(self.models, _coeffs)]
        self.norm_coeff = _coeffs.sum()
        _coeffs /= self.norm_coeff
        self.__name__ = ' + '.join([(model / self.norm_coeff).__name__ for model in self.models])

    def predict(self, X):
        """
        Standard prediction method.
        :param X: data matrix
        :return:
        """
        res = 0
        for model in self.models:
            res += model.predict(X)
        return res / self.norm_coeff

    def __mul__(self, x):
        return Ensemble([model * x for model in self.models])

    def __truediv__(self, x):
        assert x != 0
        return Ensemble([model / x for model in self.models])


class CustomModel(ArithmeticMixin):
    def __init__(self, model, tracked_params=[], short_name=None, task='bc'):
        self.estimator = model
        self.short_name = (short_name if short_name is not None else self.get_short_name(model))
        self.tracked_params = tracked_params
        self.fit = self.estimator.fit
        self.task = task

    @property
    def __name__(self):
        self.params = {key: self.estimator.get_params()[key] for key in self.tracked_params if
                       key in self.estimator.get_params()}
        return f"{self.short_name}_{sha256((json.dumps(self.params, sort_keys=True)).encode()).hexdigest()[-2:] if self.params else 'default'}"

    @staticmethod
    def get_short_name(instance):
        return instance.__class__.__name__.lower().replace('classifier', '_clf').replace('regressor', '_rg')

    def __dir__(self):
        return set(dir(super())) | set(dir(self.estimator))

    def __getattr__(self, key):
        return getattr(self.estimator, key)

    def __setattr__(self, key, value):
        if 'estimator' in self.__dict__ and key in dir(self.estimator):
            setattr(self.estimator, key, value)
        else:
            super().__setattr__(key, value)

    def predict(self, X, **kw):
        if self.task == 'bc':
            return self.estimator.predict_proba(X, **kw)[:, 1]
        elif self.task == 'c':
            return self.estimator.predict_proba(X, **kw)
        elif self.task == 'r':
            return self.estimator.predict(X, **kw)


def model(model, tracked_params=[], short_name=None, task='bc'):
    """
    Creates a custom but trackable model.
    :param model: model object
    :param tracked_params: parameters to be tracked
    :param short_name: short name for your model, like 'rf' for RandomForestClassifier
    :param task: 'bc' for binary classification,
                 'c'  for classification,
                 'r'  for regression
    :return:
    """
    return CustomModel(model, tracked_params, short_name, task)