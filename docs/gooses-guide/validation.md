# Validation

Usually you will just do:

```python
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import StratifiedKFold

skf = StratifiedKFold(5, True, 42)
val = Validator(skf, roc_auc_score)
```

`Validator` has only one method: `val.score(model, feature_set)` which trains the model using features from the feature set using CV split defined by its splitter. It evaluates each fold using given metric, and saves OOF-predictions. After validation trained models and the feature set are packed into `Experiment` and placed to `kts.leaderboard`  with regards to the score.

Each experiment is given an identifier. Use it to access experiments:

```python
exp = lb['EBC23D']     # kts.lb is an alias for kts.leaderboard
exp                    # Experiment has a nice repr
exp.predict(test_df)   # Experiment will do all the FE and predictions 
                       # for any DataFrame whose columns are the same as of one used for fitting
exp.feature_importances()
```



