seed = 31337
import numpy as np
np.random.seed(seed)
seeds = np.random.randint(100, size=10)

storage_path = '../storage/'

@property
def feature_path(config):
    return storage_path + 'features/'

import mprop
mprop.init()