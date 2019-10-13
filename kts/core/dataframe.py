from copy import deepcopy

import pandas as pd


class DataFrame(pd.DataFrame):
    """A wrapper over the standard DataFrame class.
    Complements it with .train and .encoders attributes.
    
    This class is implemented to provide an indicator
    for functions whether they serve a train or test call and let propagate
    this indicator further to inner functions.

    Example:
        ```
        def a(df):
            res = stl.empty_like(df)
            tmp = b(df)
            res['res'] = b['b'] ** 2
            return res
        ```
    Args:

    Returns:

    """

    _metadata = ["train", "encoders", "slice_id"]
    _accessors = frozenset(_metadata)

    def __init__(self, df, train=None, encoders=None, slice_id=None, **kwargs):
        if isinstance(df, DataFrame):
            pd.DataFrame.__init__(self, df, **kwargs)
            self.slice_id = (df.slice_id if isinstance(slice_id, type(None)) else slice_id)
            self.train = df.train if isinstance(train, type(None)) else train
            self.encoders = (df.encoders if isinstance(encoders, type(None)) else encoders)
        else:
            pd.DataFrame.__init__(self, df, **kwargs)
            self.slice_id = "0" * 16 if isinstance(slice_id, type(None)) else slice_id
            self.train = False if isinstance(train, type(None)) else train
            self.encoders = dict() if isinstance(encoders, type(None)) else encoders

    def __copy__(self, deep=False):
        if not deep:
            return DataFrame(
                df=self,
                slice_id=self.slice_id,
                train=self.train,
                encoders=deepcopy(self.encoders),
            )
        else:
            return DataFrame(
                df=self.copy(deep=True),
                slice_id=self.slice_id,
                train=self.train,
                encoders=deepcopy(self.encoders),
            )

    @property
    def _constructor(self):
        """ """
        return DataFrame


def link(df, ktdf):
    """

    Args:
      df: 
      ktdf: 

    Returns:

    """
    assert isinstance(
        ktdf, DataFrame), "Second dataframe should be of type KTDF, not pd.DF"
    return DataFrame(df=df,
                     train=ktdf.train,
                     encoders=ktdf.encoders,
                     slice_id=ktdf.slice_id)
