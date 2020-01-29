from kts.core import ui
from kts.core.cache import CachedMapping, CachedList
from kts.validation.experiment import Experiment

experiments = CachedMapping('experiments')


class Leaderboard(ui.HTMLRepr):
    def __init__(self, name='main'):
        self.name = name
        self.aliases = CachedList(f'lb_{name}')

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
        return experiments[key]

    def __dir__(self):
        return list(experiments.keys())

    def __contains__(self, key):
        return key in experiments

    def __len__(self):
        return len(self.aliases)
    
    def register(self, experiment):
        assert experiment.id not in experiments
        experiments[experiment.id] = experiment
        self.aliases[experiment.id] = experiment.alias

    @property
    def sorted_aliases(self):
        res = list(self.aliases)
        return sorted(res, key=lambda e: e.score)
    
    @property
    def html(self):
        return ui.Leaderboard(self.sorted_aliases).html

leaderboard = Leaderboard('main')  # TODO: sync with lbs

class LeaderboardList:
    def __init__(self):
        self.leaderboards = CachedMapping('leaderboards')

    def __getitem__(self, key):
        return self.leaderboards[key]

    def __getattr__(self, key):
        return self.leaderboards[key]

    def __dir__(self):
        return list(self.leaderboards.keys())

    def register(self, experiment: Experiment, leaderboard_name: str):
        if leaderboard_name not in self.leaderboards:
            self.leaderboards[leaderboard_name] = Leaderboard(leaderboard_name)
        self.leaderboards[leaderboard_name].register(experiment)

leaderboard_list = LeaderboardList()