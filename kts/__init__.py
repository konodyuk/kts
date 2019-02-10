import sys
import os
sys.path.insert(0, '.')

print(sys.argv)

if True or not sys.argv[0]:
    from .cli import check_file_system
    check_file_system()
    from .feature.decorators import preview, register, deregister, dropper, selector
    from .feature.stl import *
    from .feature import stl
    from .feature.storage import feature_list as features
    from .validation.experiment import experiment_list as experiments

    VERSION_FILE = os.path.join(os.path.dirname(__file__), 'VERSION')
    with open(VERSION_FILE) as f:
        __version__ = f"0.0.{int(f.read().strip()) + 1}"