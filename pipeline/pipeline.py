from .model import *

class Pipeline(Model):
    def __init__(self, model, featureset, splitter):
        self.model = model
        self.featureset = featureset
        
    def fit(self, df):
        