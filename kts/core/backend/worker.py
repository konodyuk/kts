import os
import traceback
from typing import Dict

import pandas as pd
import ray

import kts.core.backend.signal as rs
from kts.core.backend import signal, address_manager
from kts.core.backend.progress import ProgressSignal
from kts.core.backend.signal import RunPID
from kts.core.backend.stats import Stats
from kts.core.frame import KTSFrame


@ray.remote(num_return_vals=3, max_retries=0)
def worker(self, *args, df: pd.DataFrame, meta: Dict):
    assert 'run_manager' not in meta
    assert 'report' not in meta
    assert 'pid' in meta
    signal.pid = meta['pid']
    address_manager.pid = meta['pid']
    kf = KTSFrame(df, meta=meta)
    kf.__meta__['remote'] = True
    return_state = kf._train
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

    if 'columns' in dir(res_kf) and '__columns' not in kf._state:
        kf._state['__columns'] = list(res_kf.columns)

    if return_state:
        res_state = kf._state
    else:
        res_state = None
    if self.verbose:
        rs.send(ProgressSignal(1, 1, stats.data['took'], None, None))
    return res_kf, res_state, stats.data
