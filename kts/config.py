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

cache_mode = 'disk_and_ram'  # "disk", "disk_and_ram", "ram"
cache_policy = 'everything'  # "everything", "service"

LB_DF_NAME = '__leaderboard'

service_names = [LB_DF_NAME]


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


if cache_mode in ['disk', 'disk_and_ram']:
    sys.path.insert(0, root_dir)
    try:
        from kts_config import *
    except:
        pass


# So dumb. I know.
# TODO: implement all this ... with pathlib
if not storage_path.endswith('/'):
    storage_path += '/'
