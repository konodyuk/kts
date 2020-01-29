from collections import defaultdict

import pandas as pd

from kts.util.hashing import hash_frame


class KTSFrame(pd.DataFrame):
    """A wrapper over the standard DataFrame class.
    Complements it with .train and .state/._state attributes.

    This class is implemented to provide an indicator
    for functions whether they serve a train or test call and to propagate
    this indicator to inner functions.

    You can also use .state (or ._state if the dataframe has df['state'] column) field
    to store the state of your feature between training and inference stages.

    Example:
    ```
    @register
    def tfidf(df):
        from sklearn... import TfidfVectorizer as TfIdf
        if df.train:
            enc = TfIdf()
            data = enc.fit_transform(df['text']).todense()
            df.state['tfidf'] = enc
        else:
            enc = df.state['tfidf']
            data = enc.transform(df['text']).todense()
        cols = [f'tfidf_{i}' for i in range(data.shape[1])]
        return pd.DataFrame(data=data, columns=cols, index=df.index)
    ```
    """

    _internal_names = pd.DataFrame._internal_names + ['__memoized_hash__']
    _internal_names_set = set(_internal_names)

    _metadata = ["__meta__"]
    _accessors = frozenset(_metadata)

    def __init__(self, df, meta=None, **kwargs):
        if isinstance(df, KTSFrame):
            pd.DataFrame.__init__(self, df, **kwargs)
            self.__meta__ = df.__meta__ if meta is None else meta
        else:
            pd.DataFrame.__init__(self, df, **kwargs)
            if meta is None:
                self.__meta__ = self._default_meta
            else:
                self.__meta__ = meta

    def __copy__(self, deep=False):
        if not deep:
            return self.__class__(
                df=self,
                meta=self.__meta__
            )
        else:
            return self.__class__(
                df=self.copy(deep=True),
                meta=self.__meta__
            )

    def set_scope(self, scope):
        self.__meta__['scope'] = scope

    @property
    def __states__(self):
        return self.__meta__['states']

    @property
    def _train(self):
        return self.__meta__['train']

    @property
    def train(self):
        return self.__meta__['train']

    @property
    def _fold(self):
        return self.__meta__['fold']

    @property
    def _scope(self):
        return self.__meta__['scope']

    @property
    def _remote(self):
        return self.__meta__['remote']

    @property
    def _default_meta(self):
        return {
            'states': defaultdict(dict),
            'fold': '0000',
            'scope': '__global__',
            'train': False,
            'remote': False
        }
    

    @property
    def _state_key(self):
        assert self._scope != '__global__'
        return self._scope, self._fold

    @property
    def _state(self):
        return self.__states__[self._state_key]

    @property
    def state(self):
        return self._state

    def clear_states(self) -> 'KTSFrame':
        """Leaves only necessary keys and clear states."""
        new = self.__class__(self, self._default_meta)
        for key in new.__meta__:
            new.__meta__[key] = self.__meta__[key]
        new.__meta__['states'] = defaultdict()
        return new

    # Commented out in order to keep kf.state['__columns'] = ... safe.
    # @_state.setter 
    # def _state(self, value):
    #     self.__states__[self._state_key] = value

    # @state.setter
    # def state(self, value):
    #     self._state = value

    @property
    def _constructor(self):
        return self.__class__

    def hash(self):
        if '__memoized_hash__' not in dir(self):
            self.__memoized_hash__ = hash_frame(self)
        return self.__memoized_hash__

    # def detach(self) -> 'AutonomousKTSFrame':
    #     return AutonomousKTSFrame.from_ktsframe(self)


# class AutonomousKTSFrame(KTSFrame):
#     @classmethod
#     def from_ktsframe(cls, ktsframe: KTSFrame, **kw):
#         self = cls(ktsframe)
#         state_key = ktsframe._state_key
#         super().__init__(self, ktsframe, **kw)
#         self.__autonomous_states__ = {state_key: copy(ktsframe.__states__[state_key])}
#         return self
#
#     @property
#     def __states__(self):
#         return self.__autonomous_states__
#
#     def attach(self, run_manager):
#         raise NotImplemented


# def link(df, ktdf):
#     assert isinstance(
#         ktdf, DataFrame), "Second dataframe should be a KTSFrame, not pd.DataFrame"
#     return DataFrame(df=df,
#                      train=ktdf.train,
#                      encoders=ktdf.encoders,
#                      fold=ktdf.fold)
