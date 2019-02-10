import sys

# from . import cli

print(sys.argv)

if True or not sys.argv[0]:
    from . import cli
    # from cli import check_file_system
    cli.check_file_system()
    from .feature.decorators import preview, register, deregister, dropper, selector
    from .feature.stl import *
    from .feature import stl
    from .feature.storage import feature_list as features
    from .validation.experiment import experiment_list as experiments

    __all__ = [
        'feature',
        'features',
        'validation',
        'experiments',
        'stl',
        'zoo',
        'model',
        'pipeline',
        'storage',
        'cli'
    ]

    with open("VERSION", 'r') as f:
        __version__ = f"0.0.{int(f.read().strip()) + 1}"