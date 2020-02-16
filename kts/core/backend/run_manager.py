import time
from collections import defaultdict
from typing import Optional, Dict, Union, Tuple, Any

import ray
from ray._raylet import ObjectID
from ray.experimental import signal as rs

import kts.core
from kts.core.backend.address_manager import get_address_manager
from kts.core.backend.io import TextChunk
from kts.core.backend.progress import LocalProgressBar, ProgressSignal
from kts.core.backend.signals import filter_signals, Sync, ResourceRequest
from kts.core.cache import RunID, frame_cache, AnyFrame, CachedMapping
from kts.core.frame import KTSFrame


class Run:
    def __init__(self, res_df, res_state, stats):
        self.res_df = res_df
        self.res_state = res_state
        self.stats = stats


class RunCache:
    def __init__(self):
        pass

    def __contains__(self, key: RunID):
        return key.get_alias_name() in frame_cache

    def __setitem__(self, key: RunID, value: AnyFrame):
        frame_cache.save_run(value, key)

    def __getitem__(self, key: RunID):
        return frame_cache.load_run(key)


class RunManager:
    def __init__(self):
        self.states = CachedMapping('states')
        self.runs = RunCache()
        self.scheduled = defaultdict(Run)

    def run(self, feature_constructors, frame: KTSFrame, remote=False, ret=False, report=None) -> Optional[Dict[str, AnyFrame]]:
        if not remote:
            kts.core.backend.progress.pbar = LocalProgressBar
            kts.core.backend.progress.pbar.report = report
        if remote:
            frame = ray.put(frame.clear_states())
        else:
            frame.__meta__['run_manager'] = self
            frame.__meta__['frame_cache'] = frame_cache
            frame.__meta__['report'] = report
        results = dict()
        for feature_constructor in feature_constructors:
            if not remote:
                kts.core.backend.progress.pbar.run_id = RunID(feature_constructor.name, frame.scope, frame.hash())
            results[feature_constructor.name] = feature_constructor(frame, ret=ret)
        if ret:
            return results

    def completed(self) -> bool:
        return len(ray.wait(self.futures, num_returns=len(self.scheduled), timeout=0)[1]) == 0

    @property
    def futures(self):
        return [i.res_df for i in self.scheduled.values()]

    def find_run_id(self, oid):
        for k, v in self.scheduled:
            if v == oid:
                return k

    def filter_map_id(self, signals, signal_type):
        return {self.find_run_id(o): s for o, s in signals if isinstance(s, signal_type)}

    def supervise(self, report=None):
        while not self.completed():
            signals = rs.receive(self.futures, timeout=0)
            syncs = filter_signals(signals, Sync)
            for sync in syncs:
                self.sync(**sync.get_contents())
            resource_requests = filter_signals(signals, ResourceRequest)
            for rr in resource_requests:
                key = rr.get_contents()
                address_manager = get_address_manager()
                if ray.get(address_manager.has.remote(key)):
                    if ray.get(address_manager.isnone.remote(key)):
                        self.put_resource(key)
                else:
                    self.put_resource(key)
            progress_signals = self.filter_map_id(signals, ProgressSignal)
            for rid, ps in progress_signals.items():
                report.update(rid, **ps.get_contents())
            text_chunks = self.filter_map_id(signals, TextChunk)
            for rid, tc in text_chunks.items():
                report.update_text(rid, **tc.get_contents())

            time.sleep(0.1)

    def put_resource(self, key: Union[RunID, Tuple[str, str], str]):
        resource = self.get_resource(key)
        address_manager = get_address_manager()
        if isinstance(resource, ObjectID):
            address_manager.put.remote((key, resource))
        # if resource is not None:
        address = ray.put(resource)
        address_manager.put.remote((key, address))

    def get_resource(self, key: Union[RunID, Tuple[str, str], str]) -> Any:
        if isinstance(key, RunID):
            if key in self.runs:
                return self.runs[key] # df
            if key in self.scheduled:
                return self.scheduled[key].res_df # oid
        elif isinstance(key, tuple):
            if str(key) in self.states:
                return self.states[str(key)] # df
            if key in [i.state_id for i in self.scheduled.keys()]:
                return [v.res_state for k, v in self.scheduled.items() if k.state_id == key][0] # oid
        elif isinstance(key, str):
            if key in frame_cache:
                return frame_cache.load(key) # df
        else:
            raise TypeError(f"Unsupported key type: {type(key)}")

    def merge_scheduled(self):
        for run_id, run in self.scheduled.items():
            res_df, res_state, stats = ray.get([run.res_df, run.res_state, run.stats])
            self.sync(run_id, res_df, res_state, stats)
        self.scheduled.clear()

    def sync(self, run_id: RunID, res_df: Union[AnyFrame, ObjectID], res_state: Union[Dict, ObjectID], stats: Union[Dict, ObjectID]):
        if all(isinstance(i, ObjectID) for i in [res_df, res_state, stats]):
            # futures
            self.scheduled[run_id] = Run(res_df, res_state, stats)
        else:
            # objects
            if res_df is not None:
                self.runs[run_id] = res_df
            if res_state is not None:
                self.states[str(run_id.state_id)] = res_state
