import seaborn as sns
from matplotlib.pyplot import figure

from kts.feature_selection.selector import BuiltinImportance


def plot_importances(experiment,
                     n_best=15,
                     sort_by="mean",
                     calculator=BuiltinImportance(),
                     fontsize=12):
    """Show feature importances of an experiment.

    Args:
      experiment: Experiment instance, like lb['012ABC']
      sort_by: one of 'max', 'mean', 'min', and 'std' (Default value = 'mean')
      calculator: ImportanceCalculator instance (Default value = BuiltinImportance())
      fontsize:  (Default value = 12)
      n_best: number of features to show (Default value = 15)
    """

    AGGS = ["max", "mean", "min", "std"]
    assert sort_by in AGGS
    importances = experiment.feature_importances(
        importance_calculator=calculator)
    n_best = min(n_best, importances.shape[1])
    tmp = importances.T.join(importances.agg(AGGS).T)
    tmp.sort_values(sort_by, ascending=False, inplace=True)
    tmp = tmp.head(n_best)
    tmp = tmp.drop(AGGS, axis=1).unstack().reset_index(level=1)
    tmp.columns = ['feature', 'importance']
    figure(figsize=(8, n_best / 2.5))
    a = sns.barplot(x="importance",
                    y="feature",
                    data=tmp,
                    capsize=0.2,
                    errwidth=1.5,
                    color='gray',
                    errcolor=sns.mpl.rcParams['axes.edgecolor'])
    a.spines['top'].set_visible(False)
    a.spines['right'].set_visible(False)
    a.spines['bottom'].set_visible(False)
    a.spines['left'].set_visible(False)
    a.tick_params(labelsize=fontsize)
    a.set_xlabel("importance", fontsize=fontsize)
    a.set_ylabel("feature", fontsize=fontsize)
    return a
