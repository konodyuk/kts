from typing import Union, List, Optional

import pandas as pd

from kts.core.cache import DataFrameAlias
from kts.core.frame import KTSFrame
from kts.core.runtime import ParallelFeatureConstructor, InlineFeatureConstructor
from kts.settings import cfg


class Applier(ParallelFeatureConstructor):
    cache = False

    def __init__(self, func, parts=None, optimize=True):
        self.func = func
        if parts is None:
            parts = cfg.threads
        self.parts = parts
        self.optimize = optimize

    def map(self, df):
        if len(df) < 100 and self.optimize:
            yield (0, len(df))
        else:
            for part in range(self.parts):
                yield (part * len(df) // self.parts, (part + 1) * len(df) // self.parts)

    def compute(self, start, stop, df):
        return df.iloc[start:stop].apply(self.func, axis=1)

    def get_scope(self, start, stop):
        return f"stl_apply_{start}_{stop}"

    def reduce(self, results):
        return pd.concat(results, axis=0)


class CategoryEncoder(ParallelFeatureConstructor):
    def __init__(self, encoder, columns: Union[List[str], str], targets: Optional[Union[List[str], str]] = None):
        self.encoder = encoder
        if not isinstance(columns, list):
            columns = [columns]
        self.columns = columns
        if not isinstance(targets, list):
            targets = [targets]
        self.targets = targets
    
    def map(self, df):
        for col in self.columns:
            for tar in self.targets:
                yield (col, tar)

    def compute(self, col, tar, df):
        EncoderClass = self.encoder.__class__
        if df._train:
            params = self.encoder.get_params()
            params['cols'] = [col]
            enc = EncoderClass(**params)
            res = enc.fit_transform(df[[col]], df[tar])
            df._state['enc'] = enc
        else:
            enc = df._state['enc']
            res = enc.transform(df[[col]])
        col_name = f"{col}_ce_"
        if tar is not None:
            col_name += f"{tar}_"
        enc_params = enc.get_params()
        default_params = EncoderClass().get_params()
        diff = {k: v for k, v in enc_params.items() if enc_params[k] != default_params[k] and k != 'cols'}
        col_name += f"{EncoderClass.__name__}"
        if len(diff) > 0:
            col_name += "_" + "_".join(f"{k}.{v}" for k, v in diff.items())
        res.columns = [col_name]
        return res

    def get_scope(self, col, tar):
        return f"stl_category_encoder_{col}_{tar}"

    def reduce(self, results):
        return pd.concat(results, axis=1)


class Identity(InlineFeatureConstructor):
    def __init__(self):
        pass

    def compute(self, kf: KTSFrame, ret=True):
        if ret:
            return kf

    def get_alias(self, kf: KTSFrame):
        frame_cache = kf.__meta__['frame_cache']
        alias_name = frame_cache.save(kf)
        return frame_cache.load(alias_name, ret_alias=True)


class EmptyLike(InlineFeatureConstructor):
    def __init__(self):
        pass

    def compute(self, kf: KTSFrame, ret=True):
        if ret:
            return kf[[]]

    def get_alias(self, kf: KTSFrame):
        frame_cache = kf.__meta__['frame_cache']
        alias_name = frame_cache.save(kf[[]])
        return frame_cache.load(alias_name, ret_alias=True)


class Selector(InlineFeatureConstructor):
    def __init__(self, feature_constructor, columns):
        self.feature_constructor = feature_constructor
        self.columns = columns

    def compute(self, kf: KTSFrame, ret=True):
        res = self.feature_constructor.compute(kf)
        if ret:
            return res[[self.columns]]

    def get_alias(self, kf: KTSFrame):
        alias = self.feature_constructor.get_alias(kf)
        alias = alias & self.columns
        return alias

    @property
    def cache(self):
        return self.feature_constructor.cache

    @property
    def parallel(self):
        return self.feature_constructor.parallel
    

class Dropper(InlineFeatureConstructor):
    def __init__(self, feature_constructor, columns):
        self.feature_constructor = feature_constructor
        self.columns = columns

    def compute(self, kf: KTSFrame, ret=True):
        res = self.feature_constructor.compute(kf)
        if ret:
            return res.drop(self.columns, axis=1)

    def get_alias(self, kf: KTSFrame):
        alias = self.feature_constructor.get_alias(kf)
        alias = alias - self.columns
        return alias

    @property
    def cache(self):
        return self.feature_constructor.cache

    @property
    def parallel(self):
        return self.feature_constructor.parallel


class Concat(InlineFeatureConstructor):
    def __init__(self, feature_constructors):
        self.feature_constructors = feature_constructors

    def compute(self, kf: KTSFrame, ret=True):
        parallels = [f for f in self.feature_constructors if f.parallel]
        not_parallels = [f for f in self.feature_constructors if not f.parallel]
        results = dict()
        if len(parallels) > 0:
            interim = list()
            for f in parallels:
                interim.append(f.get_futures())
            for f, (scheduled, results) in zip(parallels, interim):
                f.wait(scheduled)
            for f, (scheduled, results) in zip(parallels, interim):
                results[f.name] = f.assemble_futures(scheduled, results, kf)
        if len(not_parallels) > 0:
            for f in not_parallels:
                results[f.name] = f(kf)
        if not ret:
            return
        results_ordered = [results[f.name] for f in self.feature_constructors]
        return pd.concat(results_ordered, axis=1)

    def get_alias(self, kf: KTSFrame):
        aliases = [fc.get_alias(kf) for fc in self.feature_constructors]
        if all(isinstance(i, DataFrameAlias) for i in aliases):
            res = aliases[0]
            for alias in aliases[1:]:
                res = res.join(alias)
            return res
        else:
            frame_cache = kf.__meta__['frame_cache']
            res = frame_cache.concat(aliases)
            return res
