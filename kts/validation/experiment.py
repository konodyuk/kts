import pandas as pd
import texttable as tt
from fastprogress import progress_bar as pb

from kts.feature_selection.selector import BuiltinImportance
from kts.util.misc import hash_str
from kts.eda.importance import plot_importances
from kts.modelling.mixins import ArithmeticMixin


class Experiment(ArithmeticMixin):
    """ """
    def __init__(
            self,
            pipeline,
            oof,
            score,
            std,
            description,
            validator,
            feature_list,
            helper_list,
    ):
        self.pipeline = pipeline
        self.model = self.pipeline.models[0].model.model  # TODO: test out
        self.model_name = self.model.__class__.__name__
        self.parameters = self.model.get_params()
        self.models = [model.model.model for model in self.pipeline.models]
        self.featureset = self.pipeline.models[0].model.featureslice.featureset
        self.tie_featuresets()
        self.oof = oof
        self.score = score
        self.std = std
        self.identifier = hash_str(f"{pipeline.__name__}")[:6].upper()
        self.oof.columns = [
            col_name.replace("prediction", self.identifier)
            for col_name in self.oof.columns
        ]
        self.__doc__ = description if description is not None else "no description"
        self.__name__ = f"{self.identifier}-{round(score, 4)}-{pipeline.__name__}"
        self.validator = validator
        self.features = list(feature_list)
        self.helpers = list(helper_list)

    def __str__(self):
        string = f"({round(self.score, 5)}, std:{round(self.std, 3)}: \n\tModel: {self.pipeline.__name__})"
        return string

    def predict(self, df):
        """

        Args:
          df: 

        Returns:

        """
        return self.pipeline.predict(df)

    def __repr__(self):
        fields = {
            "Score":
            f"{round(self.score, 7)}, std: {round(self.std, 7)} ({self.validator.metric.__name__})",
            "Identifier": self.identifier,
            "Description": self.__doc__,
            # 'Model': self.model_name + f'\tx{len(self.models)}',
            # 'Model parameters': self.parameters,
            "Model": f"{self.model.__name__}\t x{len(self.models)}",
            "|- source ": self.model.
            source,  # be careful with refactoring: if you remove this space,
            "FeatureSet": self.featureset.__name__,  #
            "|- description": self.featureset.__doc__,  #
            "|- source": self.featureset.__repr__(
            ),  # both "source" rows will be considered identical
            "Splitter": self.validator.splitter,
        }

        table = tt.Texttable(max_width=80)
        for field in fields:
            table.add_row([field, fields[field]])
        return table.draw()

    def as_dict(self):
        """ """
        fields = {
            "Score": self.score,
            "std": self.std,
            "ID": self.identifier,
            "Model": self.model.__name__,
            "FS": self.featureset.__name__,
            "Description": self.__doc__,
            "FS description": self.featureset.__doc__,
            "Model source": self.model.source,
            "FS source": self.featureset.__repr__(),
            "Splitter": repr(self.validator.splitter),
        }
        for field in fields:
            fields[field] = [fields[field]]
        return fields

    def as_df(self):
        """ """
        return pd.DataFrame(self.as_dict()).set_index("ID")

    def feature_importances(self,
                            plot=False,
                            importance_calculator=BuiltinImportance(),
                            **kw):
        """

        Args:
          plot:  (Default value = False)
          importance_calculator:  (Default value = BuiltinImportance())
          **kw: 

        Returns:

        """
        if plot:
            return plot_importances(self,
                                    calculator=importance_calculator,
                                    **kw)
        res = pd.DataFrame()
        for w_model in pb(self.pipeline.models):
            fsl = w_model.model.featureslice
            model = w_model.model.model
            importances = importance_calculator.calc(model, fsl, self)
            importances = {k: [v] for k, v in importances.items()}
            res = res.append(pd.DataFrame(importances), ignore_index=True)
        return res

    def set_df(self, df_input):
        """

        Args:
          df_input: 

        Returns:

        """
        self.tie_featuresets()
        self.featureset.set_df(df_input)

    def tie_featuresets(self):
        """ """
        for i in range(len(self.pipeline.models)):
            self.pipeline.models[i].model.featureslice.featureset = self.featureset
