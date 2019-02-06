import numpy as np
import mprop

seed = 31337
np.random.seed(seed)
seeds = np.random.randint(100, size=10)

storage_path = '../storage/'
index_prefix = "__kts__index_"
preview_call = 0


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
