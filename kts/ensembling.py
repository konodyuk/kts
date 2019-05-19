from .validation.leaderboard import leaderboard as lb
from .validation import Validator
from .validation.split import Refiner
from .feature import stl
from warnings import warn


def assert_splitters(exps):
    all_splitters = set()
    for exp in exps:
        all_splitters.add(repr(exp.splitter))
        if len(all_splitters) > 1:
            raise Exception(f'Experiment {repr(exp.identifier)} has {exp.splitter} instead of {all_splitters.pop()}')


def assert_metrics(exps):
    all_metrics = set()
    for exp in exps:
        if 'source' in dir(exp.metric):
            all_metrics.add(exp.metric.source)
        else:
            all_metrics.add(exp.metric.__name__)
    if len(all_metrics) > 1:
        warn(f"Different metrics were used for scoring provided experiments: {all_metrics}."
             f" The first one will be used unless you specify it explicitly.")


def stack(ids, safe=True, inner_splitter=None, metric=None):
    experiments = lb[ids]
    if safe:
        assert_splitters(experiments)
    outer_splitter = experiments[0].splitter
    assert_metrics(experiments)
    if inner_splitter is None:
        inner_splitter = experiments[0].splitter
    refiner = Refiner(outer_splitter, inner_splitter)
    if metric is None:
        metric = experiments[0].metric
    fc_stack = stl.stack(ids)
    val_stack = Validator(refiner, metric)
    return val_stack, fc_stack
