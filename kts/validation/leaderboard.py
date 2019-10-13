import pandas as pd
from tqdm import tqdm

from kts.util import validation_utils
from kts.util.misc import captcha
from kts.config import GOAL, LB_DF_NAME
from kts.core.backend.memory import cache


class Leaderboard:
    """ """
    def __init__(self):
        if LB_DF_NAME in cache.cached_dfs():
            self._df = cache.load_df(LB_DF_NAME)
        else:
            self._df = pd.DataFrame()
            cache.cache_df(self._df, LB_DF_NAME)

    def register(self, experiment):
        """

        Args:
          experiment: 

        Returns:

        """
        if experiment.__name__ + "_exp" in cache.cached_objs():
            return
        cache.cache_obj(experiment, experiment.__name__ + "_exp")
        self.add_row(experiment.as_df())

    def reload(self):
        """ """
        self._df = cache.load_df(LB_DF_NAME)

    def add_row(self, row):
        """

        Args:
          row: 

        Returns:

        """
        self.reload()
        self._df = self._df.append(row)
        self._df = self._df.sort_values("Score",
                                        ascending=(GOAL == "MINIMIZE"))
        cache.remove_df(LB_DF_NAME)
        cache.cache_df(self._df, LB_DF_NAME)

    def __getattr__(self, item):
        return getattr(self._df, item)

    def __getitem__(self, key):
        if validation_utils.is_identifier(key):
            return validation_utils.get_experiment(key)
        elif validation_utils.is_list_of_identifiers(key):
            return [validation_utils.get_experiment(i) for i in key]
        else:
            res = self._df[key]
        if "style" in dir(res):
            return res.style
        return res

    def _repr_html_(self):
        """ """
        self.reload()
        return self._df.style._repr_html_()

    @property
    def df(self):
        """ """
        return self._df.copy()

    def refresh(self):
        """ """
        names = [name for name in cache.cached_objs() if name.endswith("_exp")]
        print(
            f"You want to refresh existing leaderboard."
            f" It will require loading ALL ({len(names)}) existing experiments "
            f"and may take time. Do you want to continue?")
        if not captcha():
            return
        self._df = pd.DataFrame()
        cache.remove_df(LB_DF_NAME)
        cache.cache_df(self._df, LB_DF_NAME)
        for name in tqdm(names):
            self.add_row(cache.load_obj(name).as_df())
        print("Done")


leaderboard = Leaderboard()
