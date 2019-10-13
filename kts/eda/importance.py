import seaborn as sns
from matplotlib.pyplot import figure

from kts.feature_selection.selector import BuiltinImportance


def plot_importances(experiment,
                     n_best=15,
                     sort_by="max",
                     calculator=BuiltinImportance(),
                     fontsize=12):
    """Visualize feature importances (max, mean and std) of an experiment using a given calculator.

    Args:
      experiment: Experiment instance, like lb['012ABC']
      sort_by: one of 'max', 'mean' and 'std' (Default value = 'max')
      calculator: ImportanceCalculator instance (Default value = BuiltinImportance())
      fontsize: return: (Default value = 12)
      n_best:  (Default value = 15)

    Returns:

    """
    assert sort_by in ["max", "mean", "std"]
    importances = experiment.feature_importances(
        importance_calculator=calculator)
    tmp = importances.agg(["max", "mean", "std"])
    tmp = tmp.T
    tmp = tmp.reset_index()
    tmp.sort_values(sort_by, ascending=False, inplace=True)
    figure(figsize=(8, n_best / 3))
    sns.barplot(x="max", y="index", data=tmp.head(n_best), alpha=0.5)
    sns.barplot(x="mean", y="index", data=tmp.head(n_best), alpha=0.7)
    a = sns.barplot(x="std", y="index", data=tmp.head(n_best), alpha=1)
    a.tick_params(labelsize=fontsize)
    a.set_xlabel("importance", fontsize=fontsize)
    a.set_ylabel("feature", fontsize=fontsize)
    return a
