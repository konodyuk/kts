# Basic Feature Engineering

{% hint style="info" %}
**Feature constructor** is a function taking dataframe as an argument and returning a dataframe with exactly the **same index** and **only new** **columns**. 
{% endhint %}

This is the simplest example: 

```python
def simple_feature(df):
    res = stl.empty_like(df)
    res['new_col'] = df['col'] ** 2
    return res
```

Add `@feature` to register it as a feature constructor:

```python
@feature
def simple_feature(df):
    res = stl.empty_like(df)
    res['new_col'] = df['col'] ** 2
    return res
```

`stl.empty_like` is an STL function. Learn more about them:

{% page-ref page="standard-library-for-feature-engineering.md" %}

The features are then collected into feature sets:

```python
fs_1 = FeatureSet([feature_1, feature_2, feature_3], targets='label', train_frame=train)
```

For a more extensive review see:

{% page-ref page="../masters-guide/stateful-features.md" %}

