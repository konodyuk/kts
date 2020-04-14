import os
from collections import defaultdict
from typing import Union, Tuple, List, Any

import ray

from kts.core.run_id import RunID

try:
    from ray.experimental import get_actor  # ray<=0.8.1
except ImportError:
    from ray.util import get_actor          # ray>=0.8.2


def _get_task_id(source):
    if type(source) is ray.actor.ActorHandle:
        return source._actor_id
    else:
        if type(source) is ray.TaskID:
            return source
        else:
            return ray._raylet.compute_task_id(source)


@ray.remote
class SignalManager:
    def __init__(self):
        self.data = defaultdict(list)

    def put(self, key, value):
        self.data[key].append([value, 0])

    def get(self, key):
        result = [value for value, seen in self.data[key] if not seen]
        for item in self.data[key]:
            item[1] = 1
        return result

    def clear(self):
        self.data.clear()


pid = os.getpid()

def get_signal_manager():
    return get_actor(f"SignalManager{pid}")

def create_signal_manager():
    return SignalManager.options(name=f"SignalManager{pid}").remote()


def send(signal):
    if ray.worker.global_worker.actor_id.is_nil():
        source_key = ray.worker.global_worker.current_task_id.hex()
    else:
        source_key = ray.worker.global_worker.actor_id.hex()
    signal_manager = get_signal_manager()
    signal_manager.put.remote(source_key, signal)


def receive(sources, timeout=None):
    signal_manager = get_signal_manager()
    task_id_to_sources = dict()
    for s in sources:
        task_id_to_sources[_get_task_id(s).hex()] = s
    results = []
    for task_id in task_id_to_sources:
        signals = ray.get(signal_manager.get.remote(task_id))
        for signal in signals:
            results.append((task_id_to_sources[task_id], signal))
    return results


class Signal:
    pass


class ErrorSignal(Signal):
    def __init__(self, error):
        self.error = error


class Sync(Signal):
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


class ResourceRequest(Signal):
    def __init__(self, key: Union[RunID, Tuple[str, str], str]):
        self.key = key

    def get_contents(self):
        return self.key


class RunPID(Signal):
    def __init__(self, pid):
        self.pid = pid

    def get_contents(self):
        return self.pid


def filter_signals(signals: List[Tuple[Any, Signal]], signal_type: type):
    return [i[1] for i in signals if isinstance(i[1], signal_type)]