import pandas as pd
from copy import deepcopy
import warnings


class SubDF(pd.DataFrame):
    def __getattr__(self, item):
        return None

    def __dict__(self):
        return dict()

# MAY WORK BUT NEEDS TESTING
# SubDF = type('SubDF', (pd.DataFrame), {})

class DataFrame(SubDF):
    """
    A wrapper over the standard DataFrame class.
    Complements it with .train and .encoders attributes.

    This class is implemented to provide an indicator
    for functions whether they serve a train or test call and let propagate
    this indicator further to inner functions.

    Attention:
    ----------
    Any attribute of type pd.DataFrame will be automatically converted to kts.DataFrame type,
    but an attribute of type 'method' will produce a pd.DataFrame:
    ```
    df = pd.DataFrame()
    ktdf = kts.DataFrame(df)
    type(ktdf)
    -> kts.DataFrame
    type(ktdf.T)
    -> pd.DataFrame  # because .T is actually a @property
    type(ktdf[['A', 'B']])
    -> kts.DataFrame
    type(ktdf.drop([])
    -> pd.DataFrame
    type(ktdf.fillna(ktdf.mean()))
    -> pd.DataFrame
    ```

    Example:
    --------
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


def link(df, ktdf):
    assert isinstance(ktdf, DataFrame), 'Second dataframe should be of type KTDF, not pd.DF'
    return DataFrame(df, train=ktdf.train, encoders=ktdf.encoders, slice_id=ktdf.slice_id)