from .modelling import ArithmeticMixin

class Pipeline(ArithmeticMixin):
    def __init__(self, model, featureslice):
        self.model = model
        self.featureslice = featureslice

    def fit(self, **kwargs):
        tmp = self.featureslice()
        self.model.fit(tmp.values, self.featureslice.target.values, **kwargs)

    def predict(self, df, **kwargs):
        return self.model.predict(self.featureslice(df).values, **kwargs)