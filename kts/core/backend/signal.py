from collections import defaultdict

import ray

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


def get_signal_manager():
    return get_actor('SignalManager')

def create_signal_manager():
    return SignalManager.options(name="SignalManager").remote()


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
