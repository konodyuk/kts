import os
import time
from collections import defaultdict
from copy import copy
from typing import Optional, Dict, Union, Tuple, Any, List

import pandas as pd
import ray
from ray._raylet import ObjectID
from ray.exceptions import RayError

import kts.core.backend.signal as rs
from kts.core.backend.address_manager import get_address_manager
from kts.core.backend.io import TextChunk
from kts.core.backend.progress import pbar, ProgressSignal
from kts.core.backend.ray_middleware import ensure_ray
from kts.core.backend.signal import Sync, ResourceRequest, RunPID, filter_signals
from kts.core.cache import frame_cache, CachedMapping, user_cache_frame
from kts.core.feature_constructor.base import BaseFeatureConstructor
from kts.core.frame import KTSFrame
from kts.core.run_id import RunID
from kts.core.types import AnyFrame
from kts.ui.feature_computing_report import SilentFeatureComputingReport


class Run:
    def __init__(self, res_df=None, res_state=None, stats=None, pid=None):
        self.res_df = res_df
        self.res_state = res_state
        self.stats = stats
        self.pid = pid


class RunCache:
    def __init__(self):
        self.states = CachedMapping('states')
        self.columns = CachedMapping('columns')
        self.stats = CachedMapping('stats')

    def get_state(self, run_id: RunID):
        return self.states[run_id.get_state_name()]

    def get_result(self, run_id: RunID):
        return frame_cache.load_run(run_id)

    def get_columns(self, name: str):
        """Returns unordered list of columns"""
        result = set()
        for state_name in self.columns:
            if RunID.from_state_name(state_name).function_name == name:
                result |= set(self.columns[state_name])
        return list(result)

    def get_raw_stats(self, name: str):
        result = dict()
        for alias_name in self.stats:
            run_id = RunID.from_alias_name(alias_name)
            if run_id.function_name == name:
                result[run_id] = self.stats[alias_name]
        return result

    def put_state(self, run_id: RunID, value):
        assert not self.has_state(run_id)
        self.states[run_id.get_state_name()] = value
        if '__columns' in value:
            self.columns[run_id.get_state_name()] = value['__columns']

    def put_result(self, run_id: RunID, value: pd.DataFrame):
        assert not self.has_result(run_id)
        frame_cache.save_run(value, run_id)

    def put_stats(self, run_id: RunID, stats):
        if run_id.get_alias_name() in self.stats:
            # refer to https://github.com/konodyuk/kts/tree/master/kts/core#caching-policy
            return
        self.stats[run_id.get_alias_name()] = stats

    def has_state(self, run_id: RunID):
        return run_id.get_state_name() in self.states

    def has_result(self, run_id: RunID):
        return frame_cache.has_run(run_id)

    def del_states(self, name: str):
        to_del = list()
        for key in self.states:
            run_id = RunID.from_state_name(key)
            if run_id.registration_name == name:
                to_del.append(key)
        for key in to_del:
            del self.states[key]

    def del_columns(self, name: str):
        to_del = list()
        for key in self.columns:
            run_id = RunID.from_state_name(key)
            if run_id.registration_name == name:
                to_del.append(key)
        for key in to_del:
            del self.columns[key]

    def del_stats(self, name: str):
        to_del = list()
        for key in self.stats:
            run_id = RunID.from_alias_name(key)
            if run_id.registration_name == name:
                to_del.append(key)
        for key in to_del:
            del self.stats[key]

    def del_results(self, name: str):
        to_del = list()
        for run_id in frame_cache.list_runs():
            if run_id.registration_name == name:
                to_del.append(run_id)
        for run_id in to_del:
            frame_cache.del_run(run_id)

    def del_feature(self, name: str):
        self.del_states(name)
        self.del_columns(name)
        self.del_stats(name)
        self.del_results(name)

run_cache = RunCache()

class RunManager:
    def __init__(self):
        self.scheduled = defaultdict(Run)

    def run(self,
            feature_constructors: List[BaseFeatureConstructor],
            frame: AnyFrame,
            *,
            train: bool,
            fold: str,
            ret: bool = False,
            report=None) -> Optional[Dict[str, AnyFrame]]:
        ensure_ray()
        if report is None:
            report = SilentFeatureComputingReport()
        frame = KTSFrame(frame)
        results = dict()
        for feature_constructor in feature_constructors:
            frame.__meta__['train'] = train
            frame.__meta__['fold'] = fold
            frame.__meta__['run_manager'] = self
            frame.__meta__['report'] = report
            frame.__meta__['pid'] = os.getpid()
            run_id = RunID(feature_constructor.name, frame._fold, frame.hash())
            with pbar.local_mode(report, run_id):
                results[feature_constructor.name] = feature_constructor(frame, ret=ret)
        if ret:
            return results

    def completed(self) -> bool:
        if len(self.futures) == 0:
            return True
        tmp = self.futures
        assert all(isinstance(i, ObjectID) for i in tmp)
        return len(ray.wait(tmp, num_returns=len(tmp), timeout=0)[1]) == 0

    @property
    def futures(self):
        return [i.stats for i in self.scheduled.values()]

    def new_signals(self):
        if len(self.futures) == 0:
            return []
        return rs.receive(self.futures, timeout=0)

    def find_run_id(self, oid):
        for k, v in self.scheduled.items():
            if v.stats == oid:
                return k

    def filter_map_id(self, signals, signal_type):
        return [(self.find_run_id(o), s) for o, s in signals if isinstance(s, signal_type)]

    def supervise(self, report=None):
        if report is None:
            report = SilentFeatureComputingReport()
        try:
            extra_iterations = 1
            while True:
                signals = self.new_signals()
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
                            address_manager.confirm.remote(key)
                    else:
                        self.put_resource(key)

                pid_signals = self.filter_map_id(signals, RunPID)
                for rid, pid_signal in pid_signals:
                    self.scheduled[rid].pid = pid_signal.get_contents()

                progress_signals = self.filter_map_id(signals, ProgressSignal)
                for rid, ps in progress_signals:
                    payload = ps.get_contents()
                    title = payload.pop('title')
                    rid = payload.pop('run_id', rid)
                    if title is not None:
                        rid = copy(rid)
                        rid.function_name += f" - {title}"
                    report.update(**payload, run_id=rid, autorefresh=False)

                text_chunks = self.filter_map_id(signals, TextChunk)
                for rid, tc in text_chunks:
                    payload = tc.get_contents()
                    rid = payload.pop('run_id', rid)
                    report.update_text(rid, **payload, autorefresh=False)

                errors = self.filter_map_id(signals, rs.ErrorSignal)
                for rid, err in errors:
                    report.update_text(rid, text='task failed', timestamp=time.time(), autorefresh=False)
                    report.update_text(rid, text=err.error.strip(), timestamp=time.time(), autorefresh=False)
                    report.refresh(force=True)

                report.refresh()
                time.sleep(0.01)
                extra_iterations -= self.completed()
                if extra_iterations < 0:
                    break
            report.refresh(force=True)
        except KeyboardInterrupt:
            self.kill_scheduled()

    def kill_scheduled(self):
        for run in self.scheduled.values():
            try:
                os.kill(run.pid, 9)
            except:
                pass

    def put_resource(self, key: Union[RunID, Tuple[str, str], str]):
        resource = self.get_resource(key)
        address_manager = get_address_manager()
        if isinstance(resource, ObjectID):
            address_manager.put.remote((key, resource, False))
            return
        is_none = resource is None
        address = ray.put(resource)
        address_manager.put.remote((key, address, is_none))

    def get_resource(self, key: Union[RunID, Tuple[str, str], str]) -> Any:
        if isinstance(key, RunID):
            if run_cache.has_result(key):
                return run_cache.get_result(key) # df
            if key in self.scheduled:
                return self.scheduled[key].res_df # oid
        elif isinstance(key, tuple):
            run_id = RunID(*key)
            if run_cache.has_state(run_id):
                return run_cache.get_state(run_id) # df
            if key in [i.state_id for i in self.scheduled.keys()]:
                return [v.res_state for k, v in self.scheduled.items() if k.state_id == key][0] # oid
        elif isinstance(key, str):
            if key in user_cache_frame:
                return user_cache_frame[key] # df
        else:
            raise TypeError(f"Unsupported key type: {type(key)}")

    def merge_scheduled(self):
        for run_id, run in self.scheduled.items():
            try:
                res_df, res_state, stats = ray.get([run.res_df, run.res_state, run.stats])
                self.sync(run_id, res_df, res_state, stats)
            except RayError:  # in case of failed task
                pass
        self.scheduled.clear()

    def sync(self, run_id: RunID, res_df: Union[AnyFrame, ObjectID], res_state: Union[Dict, ObjectID], stats: Union[Dict, ObjectID]):
        if all(isinstance(i, ObjectID) for i in [res_df, res_state, stats]):
            # futures
            self.scheduled[run_id].res_df = res_df
            self.scheduled[run_id].res_state = res_state
            self.scheduled[run_id].stats = stats
        else:
            # objects
            if run_id.fold == "preview":
                return
            if res_df is not None:
                run_cache.put_result(run_id, res_df)
            if res_state is not None:
                run_cache.put_state(run_id, res_state)
            if stats is not None:
                run_cache.put_stats(run_id, stats)

run_manager = RunManager()
