from ..model import Model
from ..storage import cache
from .. import config
import glob
from collections import MutableSequence


class Experiment(Model):
    def __init__(self, pipeline, oofs, score, std):
        self.pipeline = pipeline
        self.model = self.pipeline.models[0].model  # TODO: test out
        self.oofs = oofs
        self.score = score
        self.std = std
        self.__name__ = f"{round(score, 3)}:exp({pipeline.__name__})"

    def __str__(self):
        string = f"({round(self.score, 5)}, std:{round(self.std, 3)}: \n\tModel: {self.pipeline.__name__})"
        return string

    def predict(self, df):
        return self.pipeline.predict(df)


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
            # print(idx, file)
            experiment = cache.load_obj(file[:-4])
            self.experiments.append(experiment)
            self.name_to_idx[experiment.__name__] = idx
        self.experiments.sort(key=lambda e: e.score, reverse=True)

    def __getitem__(self, item):
        """
        Implements calling to experiments by score and name
        :param item: str(name) or float(score)
        :return: experiment or list of experiments
        """
        self.recalc()
        if isinstance(item, str):
            print('ok')
            ans = [experiment for experiment in self.experiments if experiment.__name__.count(item) > 0]
        elif isinstance(item, float):
            print('ok')
            ans = [experiment for experiment in self.experiments if round(experiment.score, 5) == round(item, 5)]
        elif isinstance(item, int):
            return self.experiments[item]
        elif isinstance(item, slice):
            print('ok', item)
            print(type(item.start))
            if type(item.start) == float:
                ans = [experiment for experiment in self.experiments if
                       item.start <= experiment.score and experiment.score < item.stop]
            else:
                return self.experiments[item]
        else:
            raise TypeError("Item must be of str, number or slice type")

        if len(ans) > 1:
            return ans
        elif len(ans) == 1:
            return ans[0]
        else:
            return

    def __repr__(self):
        self.recalc()
        string = "Experiments: [\n" + '\n'.join([experiment.__str__() for experiment in self.experiments]) + '\n]'
        return string

    # def __getitem__(self, key):
    #     self.recalc()
    #     if type(key) in [int, slice]:
    #         return self.experiments[key]
    #     elif type(key) == str:
    #         return self.experiments[self.name_to_idx[key]]
    #     else:
    #         raise TypeError('Index should be int, slice or str')

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
