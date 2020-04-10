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
from kts.ui.docstring import html_docstring, parse_to_html

empty_like = EmptyLike()
empty_like.__doc__ = """Returns an empty dataframe, preserving only index

Examples:
    >>> @feature
    ... def some_feature(df):
    ...     res = stl.empty_like(df)
    ...     res['col'] = ...
    ...     return res
"""
empty_like._repr_html_ = lambda *a: parse_to_html(empty_like.__doc__, 'empty_like docs')


identity = Identity()
identity.__doc__ = """Returns its input

Examples:
    >>> fs = FeatureSet([stl.identity, one_feature, another_feature], ...)
    >>> assert all((stl.identity & ['a', 'b'])(df) == stl.select(['a', 'b'])(df))
    >>> assert all((stl.identity - ['a', 'b'])(df) == stl.drop(['a', 'b'])(df))
"""
identity._repr_html_ = lambda *a: parse_to_html(identity.__doc__, 'identity docs')


@html_docstring
def concat(feature_constructors: List[BaseFeatureConstructor]) -> Concat:
    """Concatenates feature constructors

    Args:
        feature_constructors: list of feature constructors

    Returns:
        A single feature constructor whose output contains columns from each of the given features.

    Examples:
        >>> from category_encoders import WOEEncoder, CatBoostEncoder
        >>> stl.concat([
        ...     stl.select('Age']),
        ...     stl.category_encode(WOEEncoder(), ['Sex', 'Embarked'], 'Survived'),
        ...     stl.category_encode(CatBoostEncoder(), ['Sex', 'Embarked'], 'Survived'),
        ... ])
    """
    return Concat(feature_constructors)


@html_docstring
def apply(df: pd.DataFrame, func: Callable, parts: Optional[int] = None, optimize: bool = True, verbose: bool = False) -> pd.DataFrame:
    """Applies a function row-wise in parallel. Identical to df.apply(func, axis=1)

    Args:
        df: input dataframe
        func: function taking a pd.Series as input and returning a single value
        parts: number of parts to split the dataframe into. May be greater than the number of cores
        optimize: if set to True, then the dataframe won't be partitioned if its size is less than 100
        verbose: whether to show a progress bar for each process

    Returns:
        A dataframe whose only column contains the result of calling func for each row.

    Examples:
        >>> def func(row):
        ...     if row.Embarked == 'S':
        ...         return row.SibSp
        ...     return row.Age
        >>> stl.apply(df, func, parts=7, verbose=True)
    """
    return Applier(func, parts=parts, optimize=optimize, verbose=verbose)(df)


@html_docstring
def select(columns: List[str]) -> Selector:
    """Selects columns from a dataframe. Identical to df[columns]

    Args:
        columns: columns to select

    Returns:
        A feature constructor selecting given columns from input dataframe.

    Examples:
        >>> assert all(stl.select(['a', 'b'])(df) == df[['a', 'b']])
    """
    return Selector(identity, columns)


@html_docstring
def drop(columns: List[str]) -> Dropper:
    """Drops columns from a dataframe. Identical to df.drop(columns, axis=1)

    Args:
        columns: columns to drop

    Returns:
        A feature constructor dropping given columns from input dataframe.

    Examples:
        >>> assert all(stl.drop(['a', 'b'])(df) == df.drop(['a', 'b'], axis=1))
    """
    return Dropper(identity, columns)


@html_docstring
def category_encode(encoder, columns: Union[List[str], str], targets: Optional[Union[List[str], str]] = None) -> CategoryEncoder:
    """Encodes categorical features in parallel

    Performs both simple category encoding, such as one-hot, and various target encoding techniques.
    In case if target columns are provided, each pair (encoded column, target column) from cartesian product of
    both lists is encoded using encoder.

    Runs encoders returning one column (e.g. TargetEncoder, WOEEncoder)
    or fixed number of columns (HashingEncoder, BaseNEncoder) in parallel,
    whereas encoders whose number of output columns depends on count of unique values (HelmertEncoder, OneHotEncoder)
    are run in the main process to avoid result serialization overhead.

    Args:
        encoder: an instance of encoder from category_encoders package with predefined parameters
        columns: list of encoded columns. Treats string as a list of length 1
        targets: list of target columns. Should be provided if encoder uses target. Treats string as a list of length 1

    Returns:
        A feature constructor returning a concatenation of resulting columns.

    Examples:
        >>> from category_encoders import WOEEncoder, TargetEncoder
        >>> stl.category_encode(WOEEncoder(), ['Sex', 'Embarked'], 'Survived')
        >>> stl.category_encode(TargetEncoder(smoothing=3), ['Sex', 'Embarked'], ['Survived', 'Age'])
        >>> stl.category_encode(WOEEncoder(sigma=0.1, regularization=0.5), 'Sex', 'Survived')
    """
    enc = CategoryEncoder(encoder, columns=columns, targets=targets)
    expanding_encoders = [BackwardDifferenceEncoder, HelmertEncoder, OneHotEncoder, SumEncoder, PolynomialEncoder]
    if encoder.__class__ in expanding_encoders:
        enc.parallel = False
        enc.cache = False
    return enc


@html_docstring
def mean_encode(columns: Union[List[str], str], targets: Union[List[str], str], smoothing: float = 1.0, min_samples_leaf: int = 1) -> CategoryEncoder:
    """Performs mean target encoding in parallel

    An alias to stl.category_encode(TargetEncoder(smoothing, min_samples_leaf), columns, targets).

    Args:
        columns: list of encoded columns. Treats string as a list of length 1
        targets: list of target columns. Should be provided if encoder uses target. Treats string as a list of length 1
        smoothing: smoothing effect to balance categorical average vs prior.
            Higher value means stronger regularization.
            The value must be strictly bigger than 0.
        min_samples_leaf: minimum samples to take category average into account.

    Returns:
        A feature constructor performing mean encoding for each pair (column, target) and returning the concatenation.

    Examples:
        >>> stl.mean_encoding(['Sex', 'Embarked'], ['Survived', 'Age'])
        >>> stl.mean_encoding(['Sex', 'Embarked'], 'Survived', smoothing=1.5, min_samples_leaf=5)
    """
    enc = TargetEncoder(smoothing=smoothing, min_samples_leaf=min_samples_leaf)
    return category_encode(enc, columns=columns, targets=targets)


@html_docstring
def one_hot_encode(columns: Union[List[str], str]) -> CategoryEncoder:
    """Performs simple one-hot encoding

    An alias to stl.category_encode(OneHotEncoder(), columns).

    Args:
        columns: list of columns to be encoded. Treats string as a list of length 1

    Returns:
        A feature constructor returning a concatenation of one-hot encoding of each column.

    Examples:
        >>> stl.one_hot_encode(['Sex', 'Embarked'])
        >>> stl.one_hot_encode('Embarked')
    """
    enc = OneHotEncoder()
    return category_encode(enc, columns=columns, targets=None)


def discretize():
    raise NotImplementedError

def discretize_quantile():
    raise NotImplementedError

def standardize():
    raise NotImplementedError


@html_docstring
def stack(experiment_id: str, noise_level: float = 0, random_state: Optional[int] = None) -> Stacker:
    """Returns predictions of specified experiment as features

    For indices used for fitting the experiment returns OOF predictions.
    For unseen indices returns predictions obtained via experiment.predict().

    Args:
        experiment_id: id of the experiment at the leaderboard
        noise_level: range of noise added to predictions during train stage.
            If specified, then uniformly distributed value from range [-noise_level/2, noise_level/2]
            is added to each prediction.
        random_state: random state for random noise generator

    Returns:
        A feature constructor returning predictions of the experiment.

    Examples:
        >>> stl.stack('ABCDEF')
        >>> stl.stack('ABCDEF', noise_level=0.3, random_state=42)
        >>> stl.concat([stl.stack('ABCDEF'), stl.stack('GHIJKL')])
    """
    return Stacker(experiment_id, noise_level, random_state)
