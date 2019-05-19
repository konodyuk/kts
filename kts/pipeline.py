from .modelling import ArithmeticMixin

class Pipeline(ArithmeticMixin):
    def __init__(self, model, featureslice):
        self.model = model
        self.featureslice = featureslice
        self.__name__ = self.model.__name__ + '-' + self.featureslice.featureset.__name__

    def fit(self, **kwargs):
        X = self.featureslice().values
        y = self.featureslice.target.values
        self.model.fit(X, y, **kwargs)

    def predict(self, df, **kwargs):
        return self.model.predict(self.featureslice(df).values, **kwargs)