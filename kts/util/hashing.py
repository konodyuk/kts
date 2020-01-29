import hashlib
import json
from string import ascii_uppercase as alpha
from typing import List, Union

import numpy as np
import pandas as pd
import xxhash


def encode(x, b=26):
    if x < b:
        return alpha[x]
    else:
        return encode(x // b, b) + alpha[x % b]


def hash_str(s, l=None, base26=True):
    if not isinstance(s, str):
        s = str(s)
    # res = sha256(s.encode()).hexdigest()
    h = xxhash.xxh64()
    h.update(s)
    res = h.hexdigest()
    if base26:
        res = int(res, base=16)
        res = encode(res)
    if l is not None:
        res = res[:l]
    return res


def hash_dict(d, l=None, base26=True):
    return hash_str(json.dumps(d, sort_keys=True), l, base26)


def hash_list(a: List[Union[int, str]], l=None, base26=True):
    hashed_list = [hash_str(i) for i in a]
    return hash_str('_'.join(hashed_list), l, base26)


def hash_fold(idx_train, idx_valid):
    h = xxhash.xxh64()
    h.update(idx_train)
    h.update(idx_valid)
    return h.hexdigest() 


hash_frame_cache = dict()


def hash_frame(df):
    if id(df) in hash_frame_cache:
        return hash_frame_cache[id(df)]

    idx_hash = hashlib.sha256(pd.util.hash_pandas_object(
        df.index).values).hexdigest()

    sorted_cols = df.columns.sort_values()
    col_hash = hashlib.sha256(
        pd.util.hash_pandas_object(sorted_cols).values).hexdigest()
    try:
        hash_first, hash_last = pd.util.hash_pandas_object(
            df.iloc[[0, -1]][sorted_cols]).values
    except:
        hash_first = hashlib.sha256(
            df.iloc[[0]][sorted_cols].to_csv().encode("utf-8")).hexdigest()
        hash_last = hashlib.sha256(
            df.iloc[[-1]][sorted_cols].to_csv().encode("utf-8")).hexdigest()

    res = hashlib.sha256(np.array([idx_hash, col_hash, hash_first, hash_last])).hexdigest()
    hash_frame_cache[id(df)] = res
    return res


