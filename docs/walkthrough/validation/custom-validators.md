# Custom Validators

```python
class TimeSeriesValidator(Validator):
    def create_folds(self, feature_set, splitter):
        date = feature_set.aux['date'].sort_values().index.values
        for idx_train, idx_test in splitter.split(date):
            yield date[idx_train], date[idx_test]
```

```python
class GroupValidator(Validator):
    def create_folds(self, feature_set, splitter):
        y = feature_set.target
        group = feature_set.aux['group'].values
        for idx_train, idx_test in splitter.split(X=y, y=y, group=group):
            yield idx_train, idx_test
```

```python
class GroupMetricValidator(Validator):
    def evaluate(self, y_true, y_pred, feature_set_fold):
        group = feature_set_fold.aux['group'].values
        return self.metric(y_true, y_pred, group=group)
```



