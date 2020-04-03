import kts.ui.components as ui
import kts.ui.leaderboard
from kts.core.cache import CachedMapping
from kts.validation.experiment import Experiment

experiments = CachedMapping('experiments')


class Leaderboard(ui.HTMLRepr):
    def __init__(self, name='main', maximize=True):
        self.name = name
        self.data = CachedMapping(f'lb_{name}')
        self._maximize = maximize

    def __getitem__(self, key):
        if isinstance(key, str):
            return experiments[key]
        elif isinstance(key, int):
            return experiments[self.data[key].id]
        elif isinstance(key, slice):
            return [experiments[i.id] for i in self.data[key]]
        else:
            raise KeyError

    def __getattr__(self, key):
        if key not in experiments:
            raise AttributeError
        return experiments[key]

    def __dir__(self):
        return list(experiments.keys())

    def __contains__(self, key):
        return key in experiments

    def __len__(self):
        return len(self.data)
    
    def register(self, experiment):
        assert experiment.id not in experiments
        try:
            experiments[experiment.id] = experiment
            self.data[experiment.id] = experiment.alias
        except Exception as e:
            experiments.pop(experiment.id, None)
            self.data.pop(experiment.id, None)
            raise e

    @property
    def sorted_aliases(self):
        return sorted(self.data.values(), key=lambda e: e.score, reverse=self.maximize)

    @property
    def html(self):
        return kts.ui.leaderboard.Leaderboard(self.sorted_aliases).html

    def __reduce__(self):
        return (self.__class__, (self.name, self.maximize,))

    @property
    def maximize(self) -> bool:
        return self._maximize

    @maximize.setter
    def maximize(self, value: bool):
        leaderboard_list.data.pop(self.name)
        self._maximize = value
        leaderboard_list.data[self.name] = self


class LeaderboardList:
    def __init__(self):
        self.data = CachedMapping('leaderboard_list')

    def __getitem__(self, key):
        return self.data[key]

    def __getattr__(self, key):
        return self.data[key]

    def __dir__(self):
        return list(self.data.keys())

    def __contains__(self, key):
        return key in self.data

    def register(self, experiment: Experiment, leaderboard_name: str):
        if leaderboard_name not in self.data:
            self.data[leaderboard_name] = Leaderboard(leaderboard_name)
        self.data[leaderboard_name].register(experiment)


leaderboard_list = LeaderboardList()
if 'main' not in leaderboard_list:
    leaderboard_list.data['main'] = Leaderboard('main')
leaderboard = leaderboard_list['main']
