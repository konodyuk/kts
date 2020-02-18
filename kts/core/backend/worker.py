import os
from contextlib import redirect_stderr
from io import StringIO
from typing import Dict

import pandas as pd
import ray
import ray.experimental.signal as rs

from kts.core.backend.progress import ProgressSignal
from kts.core.backend.signals import RunPID
from kts.core.backend.stats import Stats
from kts.core.frame import KTSFrame


@ray.remote(num_return_vals=3, max_retries=0)
def worker(self, *args, df: pd.DataFrame, meta: Dict):
    rs.send(ProgressSignal(0, 1, None, None))
    rs.send(RunPID(os.getpid()))
    kf = KTSFrame(df, meta=meta)
    kf.__meta__['remote'] = True
    had_state = bool(kf.state)
    stats = Stats(df)
    with stats:
        with self.remote_io():
            with redirect_stderr(StringIO()):
                res_kf = self.compute(*args, kf)
    if had_state:
        res_state = None
    else:
        res_state = kf._state
    rs.send(ProgressSignal(1, 1, None, None))
    return res_kf, res_state, stats.data
