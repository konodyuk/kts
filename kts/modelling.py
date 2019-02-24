import numpy as np
from hashlib import sha256
import json

class Model:
    Estimator = None
    tracked_params = []
    fit_params = dict()
    short_name = 'model'

    def __init__(self, *args, **kwargs):
        self.estimator = self.Estimator(*args, **kwargs)
        self.params = {key: self.estimator.get_params()[key] for key in self.tracked_params}
        self.reset_name()
        self.is_fit = 0

    def reset_name(self):
        self.__name__ = f"{self.short_name}_{sha256((json.dumps(self.params, sort_keys=True)).encode()).hexdigest()[-2:] if self.params else 'default'}"

    def fit(self, X, y, **fit_params):
        if self.is_fit:
            raise UserWarning(
                f"This {type(self).__name__} is already fit.\nUse .warm_fit() to fit it from current state.")
        self.estimator.fit(X, y, **{key: fit_params[key]
                                    if key in fit_params
                                    else type(self).fit_params[key]
                                    for key in (set(fit_params.keys()) | set(type(self).fit_params.keys()))})
        self.is_fit = 1

    def predict(self, X, **predict_params):
        return self.estimator.predict(X, **predict_params)
    
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
        
    def __repr__(self):
        return self.__name__
        

class WeightedModel(Model): # MulNode
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
    

class Ensemble(Model): # AddNode
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


class CustomModel(Model):
    tracked_params = []
    fit_params = dict()
    short_name = 'model'

    def __init__(self, model, tracked_params=[], fit_params=dict(), short_name=None):
        self.estimator = model
        self.short_name = (short_name if short_name is not None else self.get_short_name(model))
        self.tracked_params = tracked_params
        self.fit_params = fit_params
        self.params = {key: self.estimator.get_params()[key] for key in self.tracked_params}
        self.reset_name()
        self.is_fit = 0

    @staticmethod
    def get_short_name(instance):
        return instance.__class__.__name__.lower().replace('classifier', '_clf').replace('regressor', '_rg')


def model(model, tracked_params=[], short_name=None, fit_params={}):
    return CustomModel(model, tracked_params, fit_params, short_name)