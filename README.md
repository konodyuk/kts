# kts

`kts` was created to simplify and unify process of solving machine learning competitions.

## Key Principles 
* Feature engineering is modular:
  * Each feature engineering function represents a block of features: takes an input dataframe and produces a new one with only new features
  * Such functions called feature constructors are then assebmled into feature sets which are then used for training models
  * Features are computed once, then they are loaded from cache
* A pair of a model and a feature set is validated using a given metric and cross validation splitter
* Once experiment is conducted, it is placed to your local leaderboard, trained models and sources are saved
* Each experiment is given an ID, which is used to access it
* Each experiment can produce predictions for any dataframe which has same columns as a training one: feature engineering is done automatically, then features are fed to trained models

## Features
* We support features which should be computed differently for training set and validation set: they are implemented as simply as usual ones using special syntax (`df.train` and `df.encoders` attributes of a dataframe passed to function)
* User cache: you can store any objects to access them from other notebooks of your project with `kts.save` and `kts.load`
* Standard library: some common feature generation techniques are preimplemented, like target or one hot encoding; you can use their sources to borrow best practices of writing custom feature constructors in kts style
* Designed for multiple notebook environment: cache is synchronized between notebooks, e.g. you can change source of a feature constructor in one notebook and get it automatically changed in other one; same for objects
* Stacking is as simple as `kts.stack(IDs)`: it creates a standard feature constructor which can be used for feature set creation
* Feature selection: select best features from an experiment using built-in feature importances calculator (sklearn-style) or permutation importance. You can also implement your own feature importance calculator using our base class

## Getting started
Use `$ pip3 install kts` to install the latest version from PyPI.  
Check [kts-examples](https://github.com/konodyuk/kts-examples) repo to learn basics.  

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
