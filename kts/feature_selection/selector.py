import abc
from abc import ABC

import numpy as np
from sklearn.metrics import make_scorer
from sklearn.model_selection._split import _build_repr


class ImportanceCalculator(ABC):
    """ """
    @property
    @abc.abstractmethod
    def short_name(self):
        """ """
        pass

    @abc.abstractmethod
    def calc(self, model, featureslice, experiment) -> dict:
        """

        Args:
          model: 
          featureslice: 
          experiment: 

        Returns:

        """
        raise NotImplementedError

    def __repr__(self):
        return _build_repr(self)


class BuiltinImportance(ImportanceCalculator):
    """ """

    short_name = "bltn"

    def __init__(self):
        super().__init__()
        pass

    def calc(self, model, featureslice, experiment):
        """

        Args:
          model: 
          featureslice: 
          experiment: 

        Returns:

        """
        return {
            name: imp for name, imp in zip(featureslice.columns, model.feature_importances_)
        }


# class LegacyImportance(ImportanceCalculator):
#     short_name = 'lgc'
#
#     def __init__(self, df):
#         super().__init__()
#         self.df = df
#
#     def calc(self, model, featureslice):
#         col_names = list(featureslice(df)[:5].columns)
#         return {name: imp for name, imp in zip(col_names, model.feature_importances_)}

try:
    import eli5.sklearn

    class SklearnPermutationImportance(ImportanceCalculator):
        """ """

        short_name = "perm"

        def __init__(self, n_rows=1000, n_iter=5, random_state=42):
            super().__init__()
            self.n_rows = n_rows
            self.n_iter = n_iter
            self.random_state = random_state

        def calc(self, model, featureslice, experiment):
            """

            Args:
              model: 
              featureslice: 
              experiment: 

            Returns:

            """
            if ("df_input" not in dir(featureslice.featureset)
                    or featureslice.featureset.df_input is None):
                raise AttributeError(
                    f"No input dataframe for featureset of the experiment found. "
                    f"Set it with lb['{experiment.identifier}'].set_df(df)")
            perm = eli5.sklearn.PermutationImportance(
                model,
                scoring=make_scorer(experiment.metric),
                refit=False,
                n_iter=self.n_iter,
                random_state=self.random_state,
            )
            perm.fit(
                featureslice(featureslice.idx_test[:self.n_rows]),
                featureslice.featureset.target.values[featureslice.idx_test][:self.n_rows],
            )
            return {
                name: imp
                for name, imp in zip(featureslice.columns, perm.feature_importances_)
            }

except ImportError:
    pass

try:
    from eli5.permutation_importance import get_score_importances

    class PermutationImportance(ImportanceCalculator):
        """ """

        short_name = "perm"

        def __init__(self, n_rows=1000, n_iter=5, random_state=42):
            super().__init__()
            self.n_rows = n_rows
            self.n_iter = n_iter
            self.random_state = random_state

        def calc(self, model, featureslice, experiment):
            """

            Args:
              model: 
              featureslice: 
              experiment: 

            Returns:

            """
            if ("df_input" not in dir(featureslice.featureset)
                    or featureslice.featureset.df_input is None):
                raise AttributeError(
                    f"No input dataframe for featureset of the experiment found. "
                    f"Set it with lb['{experiment.identifier}'].set_df(df)")

            def score_func(X, y):
                """

                Args:
                  X: 
                  y: 

                Returns:

                """
                return experiment.metric(y, model.predict(X))

            X = featureslice(featureslice.idx_test[:self.n_rows]).values
            y = featureslice.featureset.target.values[featureslice.idx_test][:self.n_rows]
            base_score, score_decreases = get_score_importances(
                score_func,
                X,
                y,
                n_iter=self.n_iter,
                random_state=self.random_state)
            feature_importances = np.mean(score_decreases, axis=0)
            return {
                name: imp
                for name, imp in zip(featureslice.columns, feature_importances)
            }

except ImportError:
    pass
