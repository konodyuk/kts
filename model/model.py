from skopt.space import Integer, Real, Categorical

class Model:
    search_spaces = {}
    fast_fit_params = {}
    default_params = {}
    system_params = {}
    Estimator = None
    
    def __init__(self, **kwargs): # (self, **kwargs)     ?
        self.is_fit = 0                        # self.params = kwargs ?
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
        self.est = type(self).Estimator(**self.params, **type(self).system_params)
        
    def fit(self, X, y, **kwargs):
        if self.is_fit:
            raise UserWarning(f"This {type(self).__name__} is already fit.\nUse .warm_fit() to fit it from current state.")
        self.est.fit(X, y, **kwargs)
        self.is_fit = True
        
    def __setattr__(self, name, value):
        if name == 'params':
            if self.is_fit:
                raise UserWarning(f"Can't change params of trained model")
            else:
                super().__setattr__(name, value)
                if not self.params:
                    return
                self.est = type(self).Estimator(**self.params, **type(self).system_params)
        else:
            super().__setattr__(name, value)
        
    def predict(self, X):
        return self.est.predict(X)
    
    def predict_proba(self, X):
        return self.est.predict_proba(X)
    
    def get_params(self, *args, **kwargs):
        return self.est.get_params(*args, **kwargs)
    
    def set_params(self, *args, **kwargs):
        return self.est.set_params(*args, **kwargs)