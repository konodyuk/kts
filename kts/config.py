import numpy as np
import mprop
import sys


seed = 31337
np.random.seed(seed)
seeds = np.random.randint(100, size=10)

storage_path = '../storage/'
root_dir = '../'
index_prefix = "__kts__index_"
preview_call = 0
memory_limit = 4 * (1024 ** 3)  # 4 Gb
mode = 'local'


@property
def feature_path(config):
    return storage_path + 'features/'


@property
def info_path(config):
    return storage_path + 'info/'


@property
def source_path(config):
    return storage_path + 'sources/'


@property
def experiment_path(config):
    return storage_path + 'experiment/'


mprop.init()


if mode == 'local':
    sys.path.insert(0, root_dir)
    try:
        from kts_config import *
    except:
        pass