from ..model import Model
from ..storage import cache
from .. import config
import glob

class Experiment(Model):
    def __init__(self, model, featureset, oofs, score):
        self.model = model
        self.featureset = featureset
        self.oofs = oofs
        self.score = score
        self.__name__ = f"{score}:exp({model.__name__}-{featureset.__name__})"
        
    def __str__(self):
        string = f"({round(self.score, 5)}: \n\tModel: {self.model.__name__}\n\t{self.featureset.__name__})"
        return string
    
    def predict(self, df):
        return self.model.predict(self.featureset(df))
    
from collections import MutableSequence

class ExperimentList(MutableSequence):
    def __init__(self):
        self.experiments = []
        self.name_to_idx = dict()

    def recalc(self):
        self.experiments = []
        self.name_to_idx = dict()
        files = glob.glob(config.storage_path + '*_exp_obj')
        files = [file.split('/')[-1] for file in files]
        for idx, file in enumerate(files):
            experiment = cache.load_obj(file[:-4])
            self.experiments.append(experiment)
            self.name_to_idx[experiment.__name__] = idx
        self.experiments.sort(key=lambda e: e.score, reverse=True)

    def __repr__(self):
        self.recalc()
        string = "Experiments: [\n" + '\n'.join([experiment.__str__() for experiment in self.experiments]) + '\n]'
        return string

    def __getitem__(self, key):
        self.recalc()
        if type(key) in [int, slice]:
            return self.experiments[key]
        elif type(key) == str:
            return self.experiments[self.name_to_idx[key]]
        else:
            raise TypeError('Index should be int, slice or str')

    def __delitem__(self, key):
        raise AttributeError('This object is read-only')

    def __setitem__(self, key, value):
        raise AttributeError('This object is read-only')

    def insert(self, key, value):
        raise AttributeError('This object is read-only')
    
    def register(self, experiment):
        cache.cache_obj(experiment, experiment.__name__ + '_exp')
    
    def __len__(self):
        self.recalc()
        return len(self.experiments)
    

experiment_list = ExperimentList()
