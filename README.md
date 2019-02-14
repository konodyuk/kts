## What is `kts` designed for

The framework is designed to help people during data science competitions. It has tools to make some steps of a competition workflow easier.

## Getting started

To install the package, just clone the repo to a directory included in `PYTHONPATH` or type `pip install kts` in the command line
Before starting new project, create a directory for it, go to that directory and type `kts init`

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

To test it out, use `@preview(train)` decorator from `kts.feature.decorators` (`train` is your training dataset):

``` python
@preview(train)
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

The function will also be contained in `kts.storage.features`. If you want to separate feature engineering from other steps of your pipeline, you can easily define all registered functions in a new notebook via 
``` python
kts.storage.features.define_in_scope(globals())
```

To learn more, read example notebook and source code. In the example there is a demonstration of the most of kts modules and features.
