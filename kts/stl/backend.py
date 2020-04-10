from typing import Union, List, Optional

import pandas as pd
import numpy as np

from kts.core.backend.progress import RemoteProgressBar
from kts.core.backend.util import in_worker
from kts.core.feature_constructor.base import InlineFeatureConstructor
from kts.core.feature_constructor.parallel import ParallelFeatureConstructor
from kts.core.frame import KTSFrame
from kts.settings import cfg


class Applier(ParallelFeatureConstructor):
    cache = False

    def __init__(self, func, parts=None, optimize=True, verbose=False):
        self.func = func
        if parts is None:
            parts = cfg.cpu_count
        self.parts = parts
        self.optimize = optimize
        self.verbose = verbose

    def split(self, df):
        if len(df) < 100 and self.optimize:
            yield (0, len(df))
        else:
            self.parts = min(self.parts, df.shape[0])
            for part in range(self.parts):
                yield (part * len(df) // self.parts, (part + 1) * len(df) // self.parts)

    def compute(self, start, stop, df):
        if self.verbose:
            func = self.ApplyProgressReporter(self.func, stop - start)
        else:
            func = self.func
        return df.iloc[start:stop].apply(func, axis=1)

    def get_scope(self, start, stop):
        return f"stl_apply_{start}_{stop}"

    def reduce(self, results):
        return pd.concat(results, axis=0)

    class ApplyProgressReporter:
        def __init__(self, func, total):
            self.func = func
            self.pbar = iter(RemoteProgressBar(range(total)))

        def __call__(self, *args, **kwargs):
            next(self.pbar)
            return self.func(*args, **kwargs)


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

    def split(self, df):
        for col in self.encoded_columns:
            for tar in self.targets:
                yield (col, tar)

    def compute(self, col, tar, df):
        EncoderClass = self.encoder.__class__
        X = df[[col]]
        if df._train:
            y = df[tar] if tar is not None else None
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


class Stacker(ParallelFeatureConstructor):
    parallel = False
    cache = True

    def __init__(self, id, noise_level=0, random_state=None):
        from kts.validation.leaderboard import experiments
        assert id in experiments, "No such experiment found"
        self.id = id
        self.experiment = experiments[self.id]
        self.oof = self.experiment.oof
        self.name = "stack_" + self.id
        self.source = f"stl.stack({repr(self.id)})"
        self.result_columns = list(self.oof.columns)
        self.requirements = self.experiment.requirements
        self.noise_level = noise_level
        self.random_state = random_state

    def compute(self, kf: KTSFrame):
        assert not in_worker()
        result = pd.DataFrame(index=kf.index.copy(), columns=self.result_columns)
        known_index = kf.index.intersection(self.oof.index)
        unknown_index = kf.index.difference(self.oof.index)
        if len(known_index) > 0:
            result.loc[known_index] = self.oof.loc[known_index]
        if len(unknown_index) > 0:
            pred = self.experiment.predict(kf.loc[unknown_index])
            if len(pred.shape) == 1:
                pred = pred.reshape((-1, 1))
            result.loc[unknown_index] = pred
        if kf._train and self.noise_level:
            rng = np.random.RandomState(self.random_state)
            noise = rng.rand(*result.shape)
            noise = (noise - 0.5) * self.noise_level
            result += noise
        return result

    def __reduce__(self):
        return (self.__class__, (self.id, self.noise_level, self.random_state))
