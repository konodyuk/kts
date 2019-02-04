import numpy as np
seed = 31337
np.random.seed(seed)
seeds = np.random.randint(100, size=10)

storage_path = '../storage/'
index_prefix = "__kts__index_"
test_call = 0
