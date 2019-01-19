from skopt.space import Integer, Real, Categorical

class Model:
    search_spaces = {}
    fast_fit_params = {}
    default_params = {}
    system_params = {}
    Estimator = None
    short_name = 'model'
    
    def __init__(self, **kwargs): # (self, **kwargs)     ?
        self.is_fit = 0     # self.params = kwargs ?
#         self.params = params ## TODO: make read-only
#         if not params:
#             self.params = type(self).default_params
        self.params = dict()
        self.system_params = type(self).system_params
        for param in type(self).default_params:
            if param in kwargs:
                self.params[param] = kwargs[param]
        for param in type(self).system_params:
            if param in kwargs:
                self.system_params[param] = kwargs[param]
        self.estimator = type(self).Estimator(**self.params, **type(self).system_params)
        self.reset_name()
#         self.coeff = 1 # hack to make Ensemble simplification possible
        
    def reset_name(self):
        self.__name__ = f"{type(self).short_name}_{hex(hash(frozenset(self.params.items())))[-2:]}"
        
    def fit(self, X, y, **kwargs):
        if self.is_fit:
            raise UserWarning(f"This {type(self).__name__} is already fit.\nUse .warm_fit() to fit it from current state.")
        self.estimator.fit(X, y, **kwargs)
        self.is_fit = True
        
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
        return self.estimator.predict(X)
    
#     def predict_proba(self, X):
#         return self.estimator.predict_proba(X)
    
#     def get_params(self, *args, **kwargs):
#         return self.estimator.get_params(*args, **kwargs)
    
#     def set_params(self, *args, **kwargs):
#         return self.estimator.set_params(*args, **kwargs)
    
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
    
    def preprocess_before_split(self, X):
        return X
    
    def preprocess_after_split(self, X):
        return X
        

class WeightedModel(Model): # MulNode
    def __init__(self, model, coeff):
        self.model = model
        self.coeff = coeff
        if type(self.model) == type(self):
            self.coeff *= self.model.coeff
            self.model = self.model.model
        self.__name__ = f"{self.coeff} * "
        if '+' in self.model.__name__:
            self.__name__ += f"({self.model.__name__})"
        else:
            self.__name__ += f"{self.model.__name__}"
            
    def predict(self, X):
        return self.coeff * self.model.predict(X)
    
class Ensemble(Model): # AddNode
    def __init__(self, models):
        self.models = []
        ensembles = [model for model in models if type(model) == type(self)]
        single_models = [model for model in models if type(model) != type(self)]
        _models = []
        for ensemble in ensembles:
            _models += ensemble.models
        _models += single_models
        
        def get_model_coeff(model):
            if 'model' in dir(model):
                return model.model, model.coeff
            return model, 1
        
        self.models = list(set([get_model_coeff(model)[0] for model in _models]))
        _coeffs = [0 for i in range(len(self.models))]
        for i in range(len(self.models)):
            for model in _models:
                if get_model_coeff(self.models[i])[0] == get_model_coeff(model)[0]:
                    _coeffs[i] += get_model_coeff(model)[1]
        
        self.models = [WeightedModel(model, coeff) for model, coeff in zip(self.models, _coeffs)]
                    
        self.__name__ = ' + '.join([model.__name__ for model in self.models])
        
    def predict(self, X):
        res = 0
        for model in self.models:
            res += model.predict(X)
        return res
    
    def __mul__(self, x):
        return Ensemble([model * x for model in self.models])
    
    def __truediv__(self, x):
        assert x != 0
        return Ensemble([model / x for model in self.models])