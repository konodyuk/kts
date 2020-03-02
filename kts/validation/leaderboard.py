import kts.ui.components as ui
import kts.ui.leaderboard
from kts.core.containers import CachedMapping
from kts.validation.experiment import Experiment

experiments = CachedMapping('experiments')


class Leaderboard(ui.HTMLRepr):
    def __init__(self, name='main'):
        self.name = name
        self.aliases = CachedMapping(f'lb_{name}')
        self.maximize = True

    def __getitem__(self, key):
        if isinstance(key, str):
            return experiments[key]
        elif isinstance(key, int):
            return experiments[self.aliases[key].id]
        elif isinstance(key, slice):
            return [experiments[i.id] for i in self.aliases[key]]
        else:
            raise KeyError

    def __getattr__(self, key):
        if key not in self:
            raise AttributeError
        return experiments[key]

    def __dir__(self):
        return list(experiments.keys())

    def __contains__(self, key):
        return key in experiments

    def __len__(self):
        return len(self.aliases)
    
    def register(self, experiment):
        assert experiment.id not in experiments
        try:
            experiments[experiment.id] = experiment
            self.aliases[experiment.id] = experiment.alias
        except Exception as e:
            experiments.pop(experiment.id, None)
            self.aliases.pop(experiment.id, None)
            raise e

    @property
    def sorted_aliases(self):
        res = list(self.aliases.values())
        return sorted(res, key=lambda e: e.score, reverse=self.maximize)

    @property
    def html(self):
        return kts.ui.leaderboard.Leaderboard(self.sorted_aliases).html

leaderboard = Leaderboard('main')  # TODO: sync with lbs

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
        self.data[leaderboard_name].register(experiment)  # TODO: fix, will not affect cached object

leaderboard_list = LeaderboardList()
