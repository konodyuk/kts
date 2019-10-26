import inspect
from typing import Union, List, Tuple, Optional, Dict, Any

import numpy as np
import pandas as pd

import kts.core.base_constructors
from kts.core import dataframe
from kts.core.feature_constructor import FeatureConstructor
from kts.feature_selection import BuiltinImportance
from kts.util import source_utils, cache_utils


class FeatureSet:
    """ """
    def __init__(
            self,
            fc_before: Union[FeatureConstructor, List[FeatureConstructor], Tuple[FeatureConstructor]],
            fc_after: Union[FeatureConstructor, List[FeatureConstructor], Tuple[FeatureConstructor]] = kts.core.base_constructors.empty_like,
            df_input: Optional[pd.DataFrame] = None,
            target_columns: Optional[Union[List[str], str]] = None,
            auxiliary_columns: Optional[Union[List[str], str]] = None,
            name: Optional[str] = None,
            description: Optional[str] = None,
            desc: Optional[str] = None,
            encoders: Optional[Dict[str, Any]] = None,
    ):
        if type(fc_before) == list:
            self.fc_before = kts.core.base_constructors.concat(fc_before)
        elif type(fc_before) == tuple:
            self.fc_before = kts.core.base_constructors.compose(fc_before)
        else:
            self.fc_before = fc_before
        if type(fc_after) == list:
            self.fc_after = kts.core.base_constructors.concat(fc_after)
        elif type(fc_after) == tuple:
            self.fc_after = kts.core.base_constructors.compose(fc_after)
        else:
            self.fc_after = fc_after
        self.target_columns = target_columns
        if isinstance(self.target_columns, str):
            self.target_columns = [self.target_columns]
        elif self.target_columns is None:
            self.target_columns = []
        self.auxiliary_columns = auxiliary_columns
        if isinstance(self.auxiliary_columns, str):
            self.auxiliary_columns = [self.auxiliary_columns]
        elif self.auxiliary_columns is None:
            self.auxiliary_columns = []
        self.encoders = encoders if encoders is not None else dict()
        if df_input is not None:
            self.set_df(df_input)
        self._first_name = name
        self.__doc__ = None
        if desc is not None and description is not None:
            raise ValueError(
                "desc is an alias of description. You can't use both")
        if description is not None:
            self.__doc__ = description
        elif desc is not None:
            self.__doc__ = desc
        self.altsource = None

    def set_df(self, df_input):
        """

        Args:
          df_input:

        Returns:

        """
        self.df_input = dataframe.DataFrame(df=df_input)
        self.df_input.train = True
        self.df_input.encoders = self.encoders
        self.df = self.fc_before(self.df_input)

    def __call__(self, df):
        ktdf = dataframe.DataFrame(df=df)
        ktdf.encoders = self.encoders
        return kts.core.base_constructors.merge([self.fc_before(ktdf), self.fc_after(ktdf)])

    def __getitem__(self, idx):
        if isinstance(self.df_input, type(None)):
            raise AttributeError("Input DataFrame is not defined")
        return kts.core.base_constructors.merge([
            self.df.iloc[idx],
            self.fc_after(
                dataframe.DataFrame(df=self.df_input.iloc[idx],
                                    train=1)),  # BUG: should have .train=True?
        ])  # made .train=1 only for preview purposes
        # actually, FS[a:b] functionality is made only for debug
        # why not write config.IS_PREVIEW_CALL = 1 then?

    def empty_copy(self):
        """ """
        res = FeatureSet(
            fc_before=self.fc_before,
            fc_after=self.fc_after,
            target_columns=self.target_columns,
            auxiliary_columns=self.auxiliary_columns,
            encoders=self.encoders,
            name=self.__name__,
            description=self.__doc__,
        )
        res.altsource = self.altsource
        return res

    def slice(self, idx_train, idx_test):
        """

        Args:
          idx_train:
          idx_test:

        Returns:

        """
        return FeatureSlice(self, idx_train, idx_test)

    @property
    def target(self):
        """ """
        if len(self.target_columns) > 0:
            if set(self.target_columns) < set(self.df_input.columns):
                return self.df_input[self.target_columns]
            elif set(self.target_columns) < set(self.df.columns):
                return self.df[self.target_columns]
            else:
                raise AttributeError("Target columns are neither given as input nor computed")
        else:
            raise AttributeError("Target columns are not defined.")

    @property
    def auxiliary(self):
        """ """
        if len(self.auxiliary_columns) > 0:
            if set(self.auxiliary_columns) < set(self.df_input.columns):
                return self.df_input[self.auxiliary_columns]
            elif set(self.auxiliary_columns) < set(self.df.columns):
                return self.df[self.auxiliary_columns]
            else:
                raise AttributeError("Auxiliary columns are neither given as input nor computed")
        else:
            raise AttributeError("Auxiliary columns are not defined.")

    @property
    def aux(self):
        """ """
        return self.auxiliary

    def __get_src(self, fc):
        if fc.__name__ == "empty_like":
            return "stl.empty_like"
        if not isinstance(fc, FeatureConstructor):
            return source_utils.get_source(fc)
        if fc.stl:
            return fc.source
        else:
            return fc.__name__

    def _source(self, short=True):
        """

        Args:
          short:  (Default value = True)

        Returns:

        """
        fc_before_source = self.__get_src(self.fc_before)
        fc_after_source = self.__get_src(self.fc_after)
        if short:
            fc_before_source = source_utils.shorten(fc_before_source)
            fc_after_source = source_utils.shorten(fc_after_source)
        prefix = "FeatureSet("
        fs_source = (prefix + "fc_before=" + fc_before_source + ",\n" +
                     " " * len(prefix) + "fc_after=" + fc_after_source +
                     ",\n" + " " * len(prefix) + "target_columns=" +
                     repr(self.target_columns) + ", auxiliary_columns=" +
                     repr(self.auxiliary_columns) + ")")
        return fs_source

    @property
    def source(self):
        """ """
        return self._source(short=False)

    def recover_name(self):
        """ """
        if self._first_name:
            return self._first_name
        ans = []
        frame = inspect.currentframe()
        tmp = {}
        for i in range(7):
            try:
                tmp = {**tmp, **frame.f_globals}
            except:
                pass
            try:
                frame = frame.f_back
            except:
                pass

        for k, var in tmp.items():
            if isinstance(var, self.__class__):
                if hash(self) == hash(var) and k[0] != "_":
                    ans.append(k)
        if len(ans) != 1:
            print(f"The name cannot be uniquely defined. Possible options are: {ans}. "
                  f"Choosing {ans[0]}. You can set the name manually via FeatureSet(name=...) or using .set_name(...)")
        self._first_name = ans[0]
        return self._first_name

    @property
    def __name__(self):
        return self.recover_name()

    def set_name(self, name):
        """

        Args:
          name:

        Returns:

        """
        self._first_name = name

    def __repr__(self):
        if "altsource" in self.__dict__ and self.altsource is not None:
            return self.altsource
        else:
            return self._source(short=True)

    def select(self,
               n_best,
               experiment,
               calculator=BuiltinImportance(),
               mode="max"):
        """

        Args:
          n_best:
          experiment:
          calculator:  (Default value = BuiltinImportance())
          mode:  (Default value = 'max')

        Returns:

        """
        good_features = list(
            experiment.feature_importances(
                importance_calculator=calculator).agg(mode).sort_values(
                    ascending=False).head(n_best).index)
        res = FeatureSet(
            fc_before=self.fc_before + good_features,
            fc_after=self.fc_after + good_features,
            df_input=self.df_input,
            target_columns=self.target_columns,
            auxiliary_columns=self.auxiliary_columns,
            encoders=self.encoders,
            name=f"{self.__name__}_{calculator.short_name}_{n_best}",
            description=
            f"Select {n_best} best features from {self._first_name} using {calculator.__class__.__name__}",
        )
        res.altsource = (
            self.__repr__() +
            f".select({n_best}, lb['{experiment.identifier}'], {calculator.__repr__()})"
        )
        return res


class FeatureSlice:
    """ """
    def __init__(self, featureset, slice, idx_test):
        self.featureset = featureset
        self.slice = slice
        self.idx_test = idx_test
        self.slice_id = cache_utils.get_hash_slice(slice)
        self.first_level_encoders = self.featureset.encoders
        self.second_level_encoders = {}
        self.columns = None
        # self.df_input = copy(self.featureset.df_input)

    def __call__(self, df=None):
        if isinstance(df, type(None)):
            fsl_level_df = dataframe.DataFrame(
                df=self.featureset.df_input.iloc[self.slice],
                # ALERT: may face memory leak here
                slice_id=self.slice_id,
                train=True,
                encoders=self.second_level_encoders,
            )
            result = kts.core.base_constructors.merge([
                self.featureset.df.iloc[self.slice],
                self.featureset.fc_after(fsl_level_df),
            ])
            self.columns = [
                i for i in result.columns
                if i not in self.featureset.target_columns
                and i not in self.featureset.auxiliary_columns
            ]
            return result[self.columns]
        elif (isinstance(df, slice) or isinstance(df, np.ndarray)
              or isinstance(df, list)):
            fsl_level_df = dataframe.DataFrame(
                df=self.featureset.df_input.
                iloc[df],  # ALERT: may face memory leak here
                slice_id=self.slice_id,
                train=False,
                encoders=self.second_level_encoders,
            )
            result = kts.core.base_constructors.merge([
                self.featureset.df.iloc[df],
                self.featureset.fc_after(fsl_level_df)
            ])
            for column in set(self.columns) - set(result.columns):
                result[column] = 0
            return result[self.columns]
        else:
            fs_level_df = dataframe.DataFrame(
                df=df, encoders=self.first_level_encoders)
            fsl_level_df = dataframe.DataFrame(
                df=df,
                encoders=self.second_level_encoders,
                slice_id=self.slice_id)
            result = kts.core.base_constructors.merge([
                self.featureset.fc_before(
                    fs_level_df),  # uses FeatureSet-level encoders
                self.featureset.fc_after(
                    fsl_level_df),  # uses FeatureSlice-level encoders
            ])
            for column in set(self.columns) - set(result.columns):
                result[column] = 0
            return result[self.columns]

    @property
    def target(self):
        """ """
        return self.featureset.target.iloc[self.slice]

    def compress(self):
        """ """
        self.featureset = self.featureset.empty_copy()