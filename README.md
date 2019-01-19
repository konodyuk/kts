# Kaggle Tool Set

kts is a working title, highly likely it will be changed to avoid legal consequences. 

## Getting started

To install the package, just clone the repo to a directory included in `PYTHONPATH`.

## What works by now

- Base of feature engineering submodule

## How it works

First of all, you need to import the module:

``` python
import kts
from kts import *
```

Then you should define a function to make new features based on your input dataframe:

``` python
def make_new_features(df):
    ...
```

To test it out, use `@test` decorator from `kts` or `kts.feature`:

``` python
@test
def make_new_features(df):
    ...
```

When you're sure that your function works fine, `@register` it:

``` python
@register
def make_new_features(df):
    ...
```

Since registering source of the function is stored in `storage/features` and calls are cached unless `no_cache=True` is used.

The function will also be contained in `kts.storage.feature_constructors`. If you want to separate feature engineering from other steps of your pipeline, you can easily define all registered functions in a new notebook via 
``` python
kts.storage.feature_constructors.define_in_scope(globals())
```

To learn more, read source and example notebook.
