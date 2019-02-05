class Model:
    search_spaces = {}
    system_params = {}
    system_fit_params = {}
    Estimator = None
    short_name = 'model'
    
    def __init__(self, params=None, n_jobs=-1, verbosity=False):
        """
        :param params: model hyperparameters
        :param n_jobs: number of threads to use
        :param verbosity: True or False (print logs or not)
        """
        self.is_fit = 0
        self.params = params
        self.n_jobs = n_jobs
        self.verbosity = verbosity
        if self.params:
            self.estimator = type(self).Estimator(**self.params, **type(self).system_params)
        else:
            self.estimator = type(self).Estimator(**type(self).system_params)
        self.reset_name()
        
    def reset_name(self):
        """
        Changes name according to current parameters.
        :return:
        """
        self.__name__ = f"{type(self).short_name}_{hex(hash(frozenset(self.params.items())))[-2:] if self.params else 'default'}"
        
    def fit(self, X, y, **kwargs):
        """
        Fit model. You can call this method only once
        :param X: data matrix
        :param y: target
        :param kwargs: additional params to be used for model fitting
        :return:
        """
        if self.is_fit:
            raise UserWarning(f"This {type(self).__name__} is already fit.\nUse .warm_fit() to fit it from current state.")
        fit_params = {}
        for param in type(self).system_fit_params:
            if param in kwargs:
                fit_params[param] = kwargs[param]
            else:
                fit_params[param] = type(self).system_fit_params[param]
        self._fit(X, y, **fit_params)
        self.is_fit = True

    def _fit(self, X, y, **kwargs):
        """
        This method is one of which you need to override to define a custom model.
        :param X: data matrix
        :param y: target values
        :param kwargs: additional params
        :return:
        """
        self.estimator.fit(X, y, **kwargs)
        
    def __setattr__(self, name, value):
        if name == 'params':
            if self.is_fit:
                raise UserWarning(f"Can't change params of trained model")
            else:
                super().__setattr__(name, value)
                if not self.params:
                    return
                self.estimator = type(self).Estimator(**self.params, **type(self).system_params)
                self.reset_name()
        else:
            super().__setattr__(name, value)
        
    def predict(self, X):
        """
        Standard prediction method.
        :param X: data matrix
        :return:
        """
        return self.estimator.predict(X)
    
    def __mul__(self, x):
        return WeightedModel(self, x)
    
    def __rmul__(self, x):
        return self * x
    
    def __truediv__(self, x):
        assert x != 0
        return WeightedModel(self, 1. / x)
    
    def __add__(self, other):
        assert type(self).__base__ == type(other).__base__, \
        f"Can't add {type(other).__name__} to {type(self).__name__}."
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
        _coeffs = [0 for i in range(len(self.models))]
        for i in range(len(self.models)):
            for model in _models:
                if __get_model_coeff(self.models[i])[0] == __get_model_coeff(model)[0]:
                    _coeffs[i] += __get_model_coeff(model)[1]
        
        self.models = [WeightedModel(model, coeff) for model, coeff in zip(self.models, _coeffs)]
                    
        self.__name__ = ' + '.join([model.__name__ for model in self.models])
        
    def predict(self, X):
        """
        Standard prediction method.
        :param X: data matrix
        :return:
        """
        res = 0
        for model in self.models:
            res += model.predict(X)
        return res
    
    def __mul__(self, x):
        return Ensemble([model * x for model in self.models])
    
    def __truediv__(self, x):
        assert x != 0
        return Ensemble([model / x for model in self.models])