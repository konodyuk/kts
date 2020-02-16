from typing import Dict

import pandas as pd
import ray

from kts.core.backend.stats import Stats
from kts.core.frame import KTSFrame


@ray.remote(num_return_vals=3)
def worker(self, *args, df: pd.DataFrame, meta: Dict):
    kf = KTSFrame(df, meta=meta)
    kf.__meta__['remote'] = True
    had_state = bool(kf.state)
    stats = Stats(df)
    with stats:
        # with self.remote_io():
        res_kf = self.compute(*args, kf)
    if had_state:
        res_state = None
    else:
        res_state = kf._state
    return res_kf, res_state, stats.data
