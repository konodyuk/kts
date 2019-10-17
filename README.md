![KTS logo](https://raw.githubusercontent.com/konodyuk/kts/master/docs/static/banner_alpha.png)

[![PyPI version](https://img.shields.io/pypi/v/kts.svg)](https://pypi.org/project/kts/)
[![Docs](https://img.shields.io/readthedocs/kts)](https://kts.readthedocs.io/)

**An ultimate workflow for solving machine learning competitions with tabular data.**

Install KTS with `pip install kts`. Compatible with Python 3.6+. 

## Features

-  Modular feature engineering
-  Source code tracking
-  Caching of interim results
-  Standard library for feature engineering
-  Easy customization
-  Local leaderboard
-  Leak-free stacking in one line
-  Parallel or distributed backend (feature computing/training/inference/hyperparameter tuning) -- coming soon


## Quick Start


```python
import kts
from kts import *
```

Load data from user cache:


```python
train = kts.load('train')
test = kts.load('test')
```

Create functions computing blocks of new features:


```python
@register
def feature_1(df):
    ...
    
@register
def feature_2(df):
    ...
    
@register
def feature_3(df):
    ...
```

Combine them using FeatureSet:


```python
fs_1 = FeatureSet([feature_1, feature_2, feature_3],
                  target_columns=...,
                  df_input=train)
```

Define a validation strategy:


```python
from sklearn.metrics import roc_score
from sklearn.model_selection import StratifiedKFold

skf = StratifiedKFold(10, True, 42)
val = Validator(skf, roc_score)
```

Train trackable models (built in or custom) using your features and get their IDs at the local leaderboard:


```python
from zoo.binary_classification import CatBoostClassifier, LGBMClassifier, LogisticRegression

cb = CatBoostClassifier(iterations=50)
lgb = LGBMClassifier()

summary_cb = val.score(cb, fs_1, verbose=False)
summary_lgb = val.score(cb, fs_1, verbose=False)
```

Use `kts.stack` to get leak-free validator and a feature block with the predictions of first-level models, then add this block to your set of features and train a second-level model:


```python
ids_to_stack = [summary_cb['id'], summary_lgb['id']]
val_stack, fc_stack = kts.stack(ids_to_stack)

fs_stack = FeatureSet([feature_1, feature_2, feature_3, fc_stack],
                  target_columns=...,
                  df_input=train)

logreg = LogisticRegression()
summary_logreg = val_stack.score(logreg, fs_stack)
```

Access the experiment by its ID and get final predictions for test dataframe:


```python
logreg_id  = summary_logreg['id']

logreg_exp = lb[logreg_id] # == kts.leaderboard[logreg_id]

test_predictions = logreg_exp.predict(test)
```

Check out the [docs](https://kts.readthedocs.io) for a detailed description of the features of KTS and its best practices of usage.  

## Command line interface
Use it to create a new project:
```
$ mkdir project
$ cd project
$ kts init
```
or download an example from [kts-examples](https://github.com/konodyuk/kts-examples) repo:
```
$ kts example titanic
```

## Contribution
Contact me in [Telegram](https://telegram.me/konodyuk) or [ODS Slack](https://opendatascience.slack.com/team/UC43HUBQV) to share any thoughts about the framework or examples. You're always welcome to propose new features or even implement them. 

## Acknowledgements
Core of the project was designed and implemented by the team of [Mikhail Andronov](https://github.com/Academich), [Roman Gorb](https://github.com/rvg77) and [Nikita Konodyuk](https://github.com/konodyuk) under the mentorship of [Alexander Avdyushenko](https://github.com/avalur) during a project practice held by Yandex and Higher School of Economics on 1-14 February 2019 at Educational Center «Sirius».
