import os
import traceback
from typing import Dict

import pandas as pd
import ray

import kts.core.backend.signal as rs
from kts.core.backend.progress import ProgressSignal
from kts.core.backend.signals import RunPID
from kts.core.backend.stats import Stats
from kts.core.frame import KTSFrame


@ray.remote(num_return_vals=3, max_retries=0)
def worker(self, *args, df: pd.DataFrame, meta: Dict):
    assert 'run_manager' not in meta
    assert 'report' not in meta
    kf = KTSFrame(df, meta=meta)
    kf.__meta__['remote'] = True
    had_state = bool(kf.state)
    if self.verbose:
        rs.send(ProgressSignal(0, 1, None, None, None))
        io = self.remote_io()
    else:
        io = self.suppress_io()
    rs.send(RunPID(os.getpid()))

    stats = Stats(df)
    with stats, io, self.suppress_stderr():
        try:
            res_kf = self.compute(*args, kf)
        except:
            rs.send(rs.ErrorSignal(traceback.format_exc()))
            return None, None, None

    if had_state:
        res_state = None
    else:
        res_state = kf._state
    if self.verbose:
        rs.send(ProgressSignal(1, 1, None, None, None))
    return res_kf, res_state, stats.data
