
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

