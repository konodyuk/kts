from typing import Union, List, Optional

import pandas as pd

from kts.core.feature_constructor.base import InlineFeatureConstructor
from kts.core.feature_constructor.parallel import ParallelFeatureConstructor
from kts.core.frame import KTSFrame
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
    def __init__(self,
                 encoder,
                 columns: Union[List[str], str],
                 targets: Optional[Union[List[str], str]] = None):
        self.encoder = encoder
        if not isinstance(columns, list):
            columns = [columns]
        self.encoded_columns = columns
        if not isinstance(targets, list):
            targets = [targets]
        self.targets = targets
        self.name = "category_encode_" + "_".join([self.encoder_name(), '_'] + columns + ['_'] + list(map(repr, targets)))
        self.source = "stl.category_encode(" + ", ".join([self.encoder_repr(), repr(columns), repr(targets)]) + ")"

    def map(self, df):
        for col in self.encoded_columns:
            for tar in self.targets:
                yield (col, tar)

    def compute(self, col, tar, df):
        EncoderClass = self.encoder.__class__
        X = df[[col]]
        y = df[tar] if tar is not None else None
        if df._train:
            params = self.encoder.get_params()
            params['cols'] = [col]
            enc = EncoderClass(**params)
            res = enc.fit_transform(X, y)
            df._state['enc'] = enc
        else:
            enc = df._state['enc']
            res = enc.transform(X)
        col_name = f"{col}_ce_"
        if tar is not None:
            col_name += f"{tar}_"
        col_name += self.encoder_name()
        if len(res.columns) == 1:
            res.columns = [col_name]
        else:
            res.columns = [f"{col_name}_{i}" for i in range(len(res.columns))]
        return res

    def reduce(self, results):
        return pd.concat(results, axis=1)

    def get_modified_parameters(self):
        EncoderClass = self.encoder.__class__
        enc_params = self.encoder.get_params()
        default_params = EncoderClass().get_params()
        res = {k: v for k, v in enc_params.items() if enc_params[k] != default_params[k] and k != 'cols'}
        return res

    def encoder_name(self):
        EncoderClass = self.encoder.__class__
        diff = self.get_modified_parameters()
        name = EncoderClass.__name__
        if len(diff) > 0:
            name += "_" + "_".join(f"{k}_{v}" for k, v in diff.items())
        return name

    def encoder_repr(self):
        EncoderClass = self.encoder.__class__
        diff = self.get_modified_parameters()
        res = EncoderClass.__name__
        res += "(" + ", ".join(f"{k}={repr(v)}" for k, v in diff.items()) + ")"
        return res

    def get_scope(self, col, tar):
        return f"stl_category_encoder_{self.encoder_name()}_{col}_{tar}"



class Identity(InlineFeatureConstructor):
    def __init__(self):
        self.name = 'identity'
        self.source = 'stl.identity'

    def compute(self, kf: KTSFrame, ret=True):
        if ret:
            return kf


class EmptyLike(InlineFeatureConstructor):
    def __init__(self):
        self.name = 'empty_like'
        self.source = 'stl.empty_like'

    def compute(self, kf: KTSFrame, ret=True):
        if ret:
            return kf[[]]


class Selector(InlineFeatureConstructor):
    def __init__(self, feature_constructor, columns):
        self.feature_constructor = feature_constructor
        self.selected_columns = columns
        self.name = f"{feature_constructor.name}_select_" + '_'.join(columns)

    def compute(self, kf: KTSFrame, ret=True):
        res = self.feature_constructor.compute(kf)
        if ret:
            return res[[self.selected_columns]]

    @property
    def cache(self):
        return self.feature_constructor.cache

    @property
    def parallel(self):
        return self.feature_constructor.parallel

    @property
    def source(self):
        column_intersection = set(self.selected_columns)
        column_intersection &= set(self.feature_constructor.columns)
        column_intersection = list(column_intersection)
        return f"{repr(self.feature_constructor)} & {column_intersection}"
    

class Dropper(InlineFeatureConstructor):
    def __init__(self, feature_constructor, columns):
        self.feature_constructor = feature_constructor
        self.dropped_columns = columns
        self.name = f"{feature_constructor.name}_drop_" + '_'.join(columns)

    def compute(self, kf: KTSFrame, ret=True):
        res = self.feature_constructor.compute(kf)
        if ret:
            return res.drop(self.dropped_columns, axis=1)

    @property
    def cache(self):
        return self.feature_constructor.cache

    @property
    def parallel(self):
        return self.feature_constructor.parallel

    @property
    def source(self):
        column_intersection = set(self.dropped_columns)
        column_intersection &= set(self.feature_constructor.columns)
        column_intersection = list(column_intersection)
        return f"{repr(self.feature_constructor)} - {column_intersection}"


class Concat(InlineFeatureConstructor):
    """
    Possible optimizations:
        - if in_worker() and only one is parallel, then run in the same thread
    """
    parallel = True

    def __init__(self, feature_constructors):
        self.feature_constructors = feature_constructors
        self.name = "concat_" + '_'.join(map(repr, feature_constructors))
        self.source = "stl.concat(" + ', '.join(map(repr, feature_constructors))  + ')'

    def compute(self, kf: KTSFrame, ret=True):
        parallels = [f for f in self.feature_constructors if f.parallel]
        not_parallels = [f for f in self.feature_constructors if not f.parallel]
        results = dict()
        if len(parallels) > 0:
            interim = list()
            for f in parallels:
                interim.append(f.get_futures(kf))
            self.supervise(kf)
            for f, (scheduled, dfs) in zip(parallels, interim):
                f.wait(scheduled.values())
            for f, (scheduled, dfs) in zip(parallels, interim):
                results[f.name] = f.assemble_futures(scheduled, dfs, kf)
        if len(not_parallels) > 0:
            for f in not_parallels:
                results[f.name] = f(kf)
        if not ret:
            return
        results_ordered = [results[f.name] for f in self.feature_constructors]
        return pd.concat(results_ordered, axis=1)
