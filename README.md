![KTS logo](https://raw.githubusercontent.com/konodyuk/kts/master/docs/static/banner_alpha.png)

[![PyPI version](https://img.shields.io/pypi/v/kts.svg)](https://pypi.org/project/kts/)
[![Docs](https://img.shields.io/badge/docs-draft%20version-orange)](https://docs.kts.ai/)
[![CI](https://github.com/konodyuk/kts/workflows/CI/badge.svg)](https://github.com/konodyuk/kts/actions/)
[![Codecov](https://img.shields.io/codecov/c/github/konodyuk/kts?label=core%20coverage)](https://codecov.io/gh/konodyuk/kts)
[![CodeFactor](https://www.codefactor.io/repository/github/konodyuk/kts/badge)](https://www.codefactor.io/repository/github/konodyuk/kts)

**An interactive environment for modular feature engineering, experiment tracking, feature selection and stacking.**

Install KTS with `pip install kts`. Compatible with Python 3.6+.

## Modular Feature Engineering
<p align="center">
    <br>
    <img width=600 src="https://raw.githubusercontent.com/konodyuk/kts/master/docs/static/modularity_diagram.png"/>
    <br>
    <br>
    <strong>Define features as independent blocks to organize your projects.</strong>
    <br>
</p>

## Source Code Tracking
<p align="center">
    <br>
    <img width=800 src="https://raw.githubusercontent.com/konodyuk/kts/master/docs/static/feature_constructor.png"/>
    <br>
    <br>
    <strong>Track source code of every feature and experiment to make each of them reproducible.</strong>
    <br>
</p>

## Parallel Computing and Caching
<p align="center">
    <br>
    <img width=800 src="https://raw.githubusercontent.com/konodyuk/kts/master/docs/static/feature_computing.png"/>
    <br>
    <br>
    <strong>Compute independent features in parallel. Cache them to avoid repeated computations.</strong>
    <br>
</p>

## Experiment Tracking
<p align="center">
    <br>
    <img width=800 src="https://raw.githubusercontent.com/konodyuk/kts/master/docs/static/leaderboard.png"/>
    <br>
    <br>
    <strong>Track your progress with local leaderboards.</strong>
    <br>
</p>

## Feature Selection
<p align="center">
    <br>
    <img width=800 src="https://raw.githubusercontent.com/konodyuk/kts/master/docs/static/feature_importances.png"/>
    <br><br>
    <strong>Compute feature importances and select features from any experiment <br> with <code>experiment.feature_importances()</code> and <code>experiment.select()</code>.</strong>
    <br>
</p>

## Interactivity and Rich Reports
<p align="center">
    <br>
    <img width=800 src="https://raw.githubusercontent.com/konodyuk/kts/master/docs/static/fitting.png"/>
    <br>
    <br>
    <strong>Monitor the progress of everything going on in KTS with our interactive reports. <br> From model fitting to computing feature importances.</strong>
    <br>
</p>

<br>

# Getting Started
## Titanic Tutorial
Start exploring KTS with tutorial based on [Titanic dataset](https://www.kaggle.com/c/titanic). Run notebooks interactively in Binder or just read them in NBViewer.
### 1. Feature Engineering
[![nbviewer](https://img.shields.io/badge/render-nbviewer-orange)](https://nbviewer.jupyter.org/github/konodyuk/kts/blob/dev/tutorials/titanic/notebooks/1.%20Feature%20Engineering.ipynb)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/konodyuk/kts/master?urlpath=/lab/tree/tutorials/titanic/notebooks/1.%20Feature%20Engineering.ipynb)
- [Modular Feature Engineering in 30 seconds](https://nbviewer.jupyter.org/github/konodyuk/kts/blob/dev/tutorials/titanic/notebooks/1.%20Feature%20Engineering.ipynb#Modular-Feature-Engineering-in-30-seconds)
- [Decorators reference](https://nbviewer.jupyter.org/github/konodyuk/kts/blob/dev/tutorials/titanic/notebooks/1.%20Feature%20Engineering.ipynb#Decorators)
- [Feature Types](https://nbviewer.jupyter.org/github/konodyuk/kts/blob/dev/tutorials/titanic/notebooks/1.%20Feature%20Engineering.ipynb#Feature-Types)
    - [Regular Features](https://nbviewer.jupyter.org/github/konodyuk/kts/blob/dev/tutorials/titanic/notebooks/1.%20Feature%20Engineering.ipynb#Regular-Features)
    - [Features Using External Frames](https://nbviewer.jupyter.org/github/konodyuk/kts/blob/dev/tutorials/titanic/notebooks/1.%20Feature%20Engineering.ipynb#Features-Using-External-Frames)
    - [Stateful Features](https://nbviewer.jupyter.org/github/konodyuk/kts/blob/dev/tutorials/titanic/notebooks/1.%20Feature%20Engineering.ipynb#Stateful-Features)
    - [Generic Features](https://nbviewer.jupyter.org/github/konodyuk/kts/blob/dev/tutorials/titanic/notebooks/1.%20Feature%20Engineering.ipynb#Generic-Features)
- [Standard Library](https://nbviewer.jupyter.org/github/konodyuk/kts/blob/dev/tutorials/titanic/notebooks/1.%20Feature%20Engineering.ipynb#Standard-Library)
- [Feature Set](https://nbviewer.jupyter.org/github/konodyuk/kts/blob/dev/tutorials/titanic/notebooks/1.%20Feature%20Engineering.ipynb#Feature-Set)

### 2. Modelling
[![nbviewer](https://img.shields.io/badge/render-nbviewer-orange)](https://nbviewer.jupyter.org/github/konodyuk/kts/blob/dev/tutorials/titanic/notebooks/2.%20Modelling.ipynb)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/konodyuk/kts/master?urlpath=/lab/tree/tutorials/titanic/notebooks/2.%20Modelling.ipynb)
- [Models](https://nbviewer.jupyter.org/github/konodyuk/kts/blob/dev/tutorials/titanic/notebooks/2.%20Modelling.ipynb#Models)
- [Validation](https://nbviewer.jupyter.org/github/konodyuk/kts/blob/dev/tutorials/titanic/notebooks/2.%20Modelling.ipynb#Validation)
- [Leaderboard](https://nbviewer.jupyter.org/github/konodyuk/kts/blob/dev/tutorials/titanic/notebooks/2.%20Modelling.ipynb#Leaderboard)
    - [Multiple Leaderboards](https://nbviewer.jupyter.org/github/konodyuk/kts/blob/dev/tutorials/titanic/notebooks/2.%20Modelling.ipynb#Multiple-Leaderboards)
- [Experiments](https://nbviewer.jupyter.org/github/konodyuk/kts/blob/dev/tutorials/titanic/notebooks/2.%20Modelling.ipynb#Experiments)
    - [Inference](https://nbviewer.jupyter.org/github/konodyuk/kts/blob/dev/tutorials/titanic/notebooks/2.%20Modelling.ipynb#Inference)
    - [Feature Importances](https://nbviewer.jupyter.org/github/konodyuk/kts/blob/dev/tutorials/titanic/notebooks/2.%20Modelling.ipynb#Feature-Importances)
- [Custom Models](https://nbviewer.jupyter.org/github/konodyuk/kts/blob/dev/tutorials/titanic/notebooks/2.%20Modelling.ipynb#Custom-Models)

### 3. Stacking
[![nbviewer](https://img.shields.io/badge/render-nbviewer-orange)](https://nbviewer.jupyter.org/github/konodyuk/kts/blob/dev/tutorials/titanic/notebooks/3.%20Stacking.ipynb)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/konodyuk/kts/master?urlpath=/lab/tree/tutorials/titanic/notebooks/3.%20Stacking.ipynb)
- [stl.stack](https://nbviewer.jupyter.org/github/konodyuk/kts/blob/dev/tutorials/titanic/notebooks/3.%20Stacking.ipynb#stl.stack)
- [Anti-overfitting](https://nbviewer.jupyter.org/github/konodyuk/kts/blob/dev/tutorials/titanic/notebooks/3.%20Stacking.ipynb#Anti-overfitting)
    - [Noise](https://nbviewer.jupyter.org/github/konodyuk/kts/blob/dev/tutorials/titanic/notebooks/3.%20Stacking.ipynb#Noise)
    - [Refiner](https://nbviewer.jupyter.org/github/konodyuk/kts/blob/dev/tutorials/titanic/notebooks/3.%20Stacking.ipynb#Refiner)
- [Deep Stacking](https://nbviewer.jupyter.org/github/konodyuk/kts/blob/dev/tutorials/titanic/notebooks/3.%20Stacking.ipynb#Deep-Stacking)
## Documentation
Check out [docs.kts.ai](http://docs.kts.ai) for a more detailed description of KTS features and interfaces
## Inline Docs
Most of our functions and classes have rich docstrings. Read them right in your notebook, without interruption.

---

# Acknowledgements
MVP of the project was designed and implemented by the team of [Mikhail Andronov](https://github.com/Academich), [Roman Gorb](https://github.com/rvg77) and [Nikita Konodyuk](https://github.com/konodyuk) under the mentorship of [Alexander Avdyushenko](https://github.com/avalur) during a project practice held by Yandex and Higher School of Economics on 1-14 February 2019 at Educational Center «Sirius».
