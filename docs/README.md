# Introduction

{% hint style="info" %}
This is a very short draft of KTS docs created for testing purposes. Therefore, explanations are mostly omitted. Please pay attention to code samples instead.
{% endhint %}

It is divided into three sections: Goose's guide contains basic features and guidelines, Master's guide is about powerful features and extensions of those introduced in the previous section. The Grandmaster's section is definitely not for everyday usage, but some of the stuff described there may be useful if you want to become a GM.

## Features

### Modular Feature Engineering

The features are defined as separate functions which are then collected into feature sets. Their independence allows for parallel computing and caching interim results, and makes project structure more neat.

#### Parallel Computing

#### Cache

### Automatic End-to-End Inference

Just use `experiment.predict(test_frame)` to perform all feature engineering and feed it into the models to get final predictions for a new dataframe.

### One-like Stacking

Use `kts.stack(experiment_id)` to create a feature constructor to pass into feature sets as a regular feature.

### Source Code Tracking

### Rich Reports

### Local Leaderboards

### Experiment Comparison

Use `kts.diff(exp_1, exp_2)` to compare two experiments: it will show difference in their features, models and hyperparameters.

