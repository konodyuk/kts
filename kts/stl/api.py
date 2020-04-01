from typing import List, Callable, Optional, Union

import pandas as pd
from category_encoders import (
    BackwardDifferenceEncoder,
    HelmertEncoder,
    OneHotEncoder,
    SumEncoder,
    PolynomialEncoder,
    TargetEncoder,
)

from kts.core.feature_constructor.base import BaseFeatureConstructor, Selector, Dropper
from kts.stl.backend import EmptyLike, Identity, Concat, Applier, CategoryEncoder, Stacker

empty_like = EmptyLike()
identity = Identity()

def concat(feature_constructors: List[BaseFeatureConstructor]):
    return Concat(feature_constructors)

def apply(df: pd.DataFrame, func: Callable, parts: Optional[int] = None, optimize: bool = True, verbose: bool = False):
    return Applier(func, parts=parts, optimize=optimize, verbose=verbose)(df)

def select(columns: List[str]):
    return Selector(identity, columns)

def drop(columns: List[str]):
    return Dropper(identity, columns)

def category_encode(encoder, columns: Union[List[str], str], targets: Optional[Union[List[str], str]] = None):
    enc = CategoryEncoder(encoder, columns=columns, targets=targets)
    expanding_encoders = [BackwardDifferenceEncoder, HelmertEncoder, OneHotEncoder, SumEncoder, PolynomialEncoder]
    if encoder.__class__ in expanding_encoders:
        enc.parallel = False
        enc.cache = False
    return enc

def mean_encode(columns: Union[List[str], str], targets: Union[List[str], str], smoothing: float = 1.0, min_samples_leaf: int = 1):
    enc = TargetEncoder(smoothing=smoothing, min_samples_leaf=min_samples_leaf)
    return category_encode(enc, columns=columns, targets=targets)

def one_hot_encode(columns: Union[List[str], str]):
    enc = OneHotEncoder()
    return category_encode(enc, columns=columns, targets=None)

def discretize():
    raise NotImplemented

def discretize_quantile():
    raise NotImplemented

def standardize():
    raise NotImplemented

def stack(experiment_id):
    return Stacker(experiment_id)
