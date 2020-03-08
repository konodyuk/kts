from typing import Union, Tuple, List, Any

import kts.core.backend.signal as rs
from kts.core.run_id import RunID


class Sync(rs.Signal):
    def __init__(self, run_id, res_df, res_state, stats):
        self.run_id = run_id
        self.res_df = res_df
        self.res_state = res_state
        self.stats = stats

    def get_contents(self):
        return {
            'run_id': self.run_id,
            'res_df': self.res_df,
            'res_state': self.res_state,
            'stats': self.stats,
        }


class ResourceRequest(rs.Signal):
    def __init__(self, key: Union[RunID, Tuple[str, str], str]):
        self.key = key

    def get_contents(self):
        return self.key


class RunPID(rs.Signal):
    def __init__(self, pid):
        self.pid = pid

    def get_contents(self):
        return self.pid


def filter_signals(signals: List[Tuple[Any, rs.Signal]], signal_type: type):
    return [i[1] for i in signals if isinstance(i[1], signal_type)]
