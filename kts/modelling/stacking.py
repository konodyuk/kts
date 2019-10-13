from warnings import warn

import kts.stl.misc
from kts.validation.leaderboard import leaderboard as lb
from kts.validation.split import Refiner
from kts.validation.validator import Validator


def assert_splitters(exps):
    """

    Args:
      exps: 

    Returns:

    """
    all_splitters = set()
    for exp in exps:
        all_splitters.add(repr(exp.splitter))
        if len(all_splitters) > 1:
            raise Exception(
                f"Experiment {repr(exp.identifier)} has {exp.splitter} instead of {all_splitters.pop()}"
            )


def assert_metrics(exps):
    """

    Args:
      exps: 

    Returns:

    """
    all_metrics = set()
    for exp in exps:
        if "source" in dir(exp.validator.metric):
            all_metrics.add(exp.validator.metric.source)
        else:
            all_metrics.add(exp.validator.metric.__name__)
    if len(all_metrics) > 1:
        warn(
            f"Different metrics were used for scoring provided experiments: {all_metrics}."
            f" The first one will be used unless you specify it explicitly.")


def stack(ids,
          safe=True,
          inner_splitter=None,
          metric=None,
          validator_class=Validator):
    """

    Args:
      ids: 
      safe:  (Default value = True)
      inner_splitter:  (Default value = None)
      metric:  (Default value = None)
      validator_class:  (Default value = Validator)

    Returns:

    """
    experiments = lb[ids]
    if safe:
        assert_splitters(experiments)
    outer_splitter = experiments[0].validator.splitter
    assert_metrics(experiments)
    if inner_splitter is None:
        inner_splitter = experiments[0].validator.splitter
    refiner = Refiner(outer_splitter, inner_splitter)
    if metric is None:
        metric = experiments[0].validator.metric
    fc_stack = kts.stl.misc.stack(ids)
    val_stack = validator_class(refiner, metric)
    return val_stack, fc_stack
