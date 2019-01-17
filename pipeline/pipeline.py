from .model import *

class Pipeline(Model):
    def __init__(self, model, features_before_split, features_after_split=[]):
        self.model = model
        self.features_before = features_before_split
        self.features_after = features_after_split
        
    def preprocess_before_split(self, x):
        
    def preprocess_after_split(self, x):
        