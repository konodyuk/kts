import pandas as pd
from copy import deepcopy
import warnings


class SubDF(pd.DataFrame):
    def __getattr__(self, item):
        return None

    def __dict__(self):
        return dict()

class DataFrame(SubDF):
    """
    A wrapper over the standard DataFrame class.
    Complements it with .train and .encoders attributes.

    This class is implemented to supply an indicator
    for functions whether they serve a train or test call and let propagate
    this indicator further to inner functions.

    Returns kts.DataFrame from any attribute that produces pd.DataFrame,
    but kts.DataFrame.drop([]) will return pd.DataFrame.
    That's not a bug, as `a(b(df))` will still propagate
    aforementioned signal, but `a(b(df.drop()))` construction is not to be
    registered as a cached function.

    Example:
    ```
    def a(df):
        res = stl.empty_like(df)
        tmp = b(df)
        res['res'] = b['b'] ** 2
        return res
    ```
    """
    def __init__(self, df, train=None, encoders=None, slice_id=None):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', UserWarning)

            if isinstance(df, DataFrame):
                super().__setattr__('df', df.df)
                super().__setattr__('slice_id', df.slice_id if isinstance(slice_id, type(None)) else slice_id)
                super().__setattr__('train', df.train if isinstance(train, type(None)) else train)  # ALERT: may cause errors during constructing like DF(DF(df), train=True)
                super().__setattr__('encoders', df.encoders if isinstance(encoders, type(None)) else encoders)  # not deepcopy to allow DF(df) init in FeatureConstructors
            else:
                super().__setattr__('df', df)
                super().__setattr__('slice_id', "0" * 16 if isinstance(slice_id, type(None)) else slice_id)
                super().__setattr__('train', False if isinstance(train, type(None)) else train)
                super().__setattr__('encoders', dict() if isinstance(encoders, type(None)) else encoders)

    def __dir__(self):
        return dir(self.df) + ['df', 'train', 'encoders', 'slice_id']

    def __copy__(self):
        return DataFrame(self.df, self.slice_id, self.train, deepcopy(self.encoders))

    def __getattr__(self, key):
        if key in ['train', 'encoders', 'df', 'slice_id']:
            return super().__getattr__(key)
        else:
            tmp = self.df.__getattr__(key)
            if isinstance(tmp, pd.DataFrame):
                return DataFrame(tmp, self.train, self.encoders)
            else:
                return tmp

    def __setattr__(self, key, value):
        if key in ['train', 'encoders', 'df', 'slice_id']:
            super().__setattr__(key, value)
        else:
            try:
                self.df.__setattr__(key, value)
            except:
                pass

    def __setitem__(self, key, value):
        self.df.__setitem__(key, value)

    def __getitem__(self, key):
        tmp = self.df.__getitem__(key)
        if isinstance(tmp, pd.DataFrame):
            return DataFrame(tmp, self.train, self.encoders)
        else:
            return tmp
