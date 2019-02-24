from .modelling import Model

class Pipeline(Model):
    def __init__(self, model, featureslice):
        self.model = model
        self.featureslice = featureslice
        self.__name__ = self.model.__name__ + ':' + "undefined_fs"

    def fit(self, **kwargs):
        tmp = self.featureslice()
        self.model.fit(tmp.values, self.featureslice.target.values, **kwargs)

    def predict(self, df, **kwargs):
        return self.model.predict(self.featureslice(df).values, **kwargs)