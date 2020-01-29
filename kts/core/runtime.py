import inspect
import time
import warnings
from abc import ABC
from collections import defaultdict
from contextlib import contextmanager, redirect_stdout
from copy import copy
from functools import wraps
from typing import Iterable, Optional, Any, Tuple

import forge
import pandas as pd
import ray
import ray.experimental.signal as rs
from ray import ObjectID

from kts.core.cache import frame_cache, CachedMapping, RunID, AnyFrame
from kts.core.frame import KTSFrame
from kts.core.ui import *


ray.init(ignore_reinit_error=True)  # needs fixing


def safe_put(kf: KTSFrame):
    h = kf.hash()
    if ray.get(address_manager.has.remote(h)):
        oid = ray.get(address_manager.get.remote(h))
    else:
        oid = ray.put(kf)
        address_manager.put.remote((h, oid))
    return oid

class Stats:
    def __init__(self, df):
        self.data = dict()
        self.data['input_shape'] = df.shape
        
    def __enter__(self):
        self.start = time.time()
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.data['took'] = time.time() - self.start


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


class AbstractProgressBar:
    def update_time(self, step, timestamp):
        if self.start is None:
            self.start = timestamp
            return
        self.took = timestamp - self.start
        if step > 0:
            self.eta = (self.total - self.step) / step * self.took


class RemoteProgressBar(AbstractProgressBar):
    _min_interval = 0.2

    def __init__(self, iterable: Iterable, total: Optional[int] = None):
    # def __init__(self, iterable, total=None):
        self.iterable = iterable
        self.total = total
        if total is None:
            try:
                self.total = len(self.iterable)
            except:
                pass
        self.took = None
        self.eta = None
        self.start = None

    def __iter__(self):
        last_update = 0.
        for i, o in enumerate(self.iterable):
            yield o
            cur = time.time()
            self.update_time(i, cur)
            if cur - last_update >= self._min_interval:
                rs.send(ProgressSignal(value=i + 1, total=self.total, took=self.took, eta=self.eta))
                last_update = cur
        rs.send(ProgressSignal(value=self.total, total=self.total, took=self.took, eta=None))


class LocalProgressBar(AbstractProgressBar):
    _min_interval = 0.2
    report = None

    def __init__(self, iterable: Iterable, total: Optional[int] = None):
    # def __init__(self, iterable, total=None):
        self.iterable = iterable
        self.total = total
        if total is None:
            try:
                self.total = len(self.iterable)
            except:
                pass
        self.took = None
        self.eta = None

    def __iter__(self):
        last_update = 0.
        for i, o in enumerate(self.iterable):
            yield o
            cur = time.time()
            self.update_time(i, cur)
            if cur - last_update >= self._min_interval:
                # rs.send(ProgressSignal(value=i + 1, total=self.total))
                self.report.update(self.run_id, self.value, self.total, self.took, self.eta)
                last_update = cur
        # rs.send(ProgressSignal(value=self.total, total=self.total))
        self.report.update(self.run_id, self.total, self.total, self.took, self.eta)


pbar = RemoteProgressBar


class TextChunk(rs.Signal):
    def __init__(self, timestamp, text):
        self.timestamp = timestamp
        self.text = text

    def get_contents(self):
        return {'timestamp': self.timestamp, 'text': self.text}


class Start(rs.Signal):
    def __init__(self):
        pass


class Finish(rs.Signal):
    def __init__(self):
        pass


class RemoteTextIO:
    def __init__(self):
        self.buf = ""

    def write(self, b):
        self.buf += b
        if b.find('\n') != -1:
            self.flush()

    def flush(self):
        if self.buf:
            rs.send(TextChunk(time.time(), self.buf))
        self.buf = ""


class LocalTextIO:
    def __init__(self, report, run_id):
        self.buf = ""
        self.report = report
        self.run_id = run_id

    def write(self, b):
        self.buf += b
        if b.find('\n') != -1:
            self.flush()

    def flush(self):
        if self.buf:
            self.report.update_text(self.run_id, timestamp=time.time(), text=self.buf)
        self.buf = ""


class ProgressSignal(rs.Signal):
    def __init__(self, value, total, took, eta):
        self.value = value
        self.total = total
        self.took = took
        self.eta = eta

    def get_percentage(self):
        return self.value / self.total * 100

    def get_contents(self):
        return {'value': self.value, 'total': self.total, 'took': self.took, 'eta': self.eta}


class ResourceRequest(rs.Signal):
    def __init__(self, key: Union[RunID, Tuple[str, str], str]):
        self.key = key

    def get_contents(self):
        return self.key

@ray.remote
class AddressManager:
    def __init__(self):
        self.data = dict()
        self.timestamps = dict()

    def get(self, key):
        return self.data[key]

    def put(self, entry):
        key, value = entry
        self.data[key] = value
        self.timestamps[key] = time.time()

    def has(self, key):
        return key in self.data

    def isnone(self, key):
        return self.data[key] is None

    def timestamp(self, key):
        return self.timestamps[key]

address_manager = AddressManager.remote()


def filter_signals(signals: List[Tuple[Any, rs.Signal]], signal_type: type):
    return [i[1] for i in signals if isinstance(i[1], signal_type)]


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
        global pbar
        if not remote:
            pbar = LocalProgressBar
            pbar.report = report
        if remote:
            frame = ray.put(frame.clear_states())
        else:
            frame.__meta__['run_manager'] = self
            frame.__meta__['frame_cache'] = frame_cache
            frame.__meta__['report'] = report
        results = dict()
        for feature_constructor in feature_constructors:
            if not remote:
                pbar.run_id = RunID(feature_constructor.name, frame.scope, frame.hash())
            results[feature_constructor.name] = feature_constructor(frame, ret=ret)
        if ret:
            return results

    def completed(self) -> bool:
        return ray.wait(self.futures, num_returns=len(self.scheduled), timeout=0)[1] == 0

    @property
    def futures(self):
        return [i.res_df for i in self.scheduled]

    def find_run_id(self, oid):
        for k, v in self.scheduled:
            if v == oid:
                return k

    def filter_map_id(self, signals, signal_type):
        return {self.find_run_id(o): s for o, s in signals if isinstance(s, signal_type)}

    def supervise(self, report=None):
        while not self.completed():
            signals = rs.receive(self.futures)
            syncs = filter_signals(signals, Sync)
            for sync in syncs:
                self.sync(**sync.get_contents())
            resource_requests = filter_signals(signals, ResourceRequest)
            for rr in resource_requests:
                key = ray.get(rr.get_contents())
                if ray.get(address_manager.has(key)):
                    if ray.get(address_manager.isnone(key)):
                        self.put_resource(key)
                else:
                    self.put_resource(key)
            progress_signals = self.filter_map_id(signals, ProgressSignal)
            for rid, ps in progress_signals.items():
                report.update(rid, **ps.get_contents())
            text_chunks = self.filter_map_id(signals, TextChunk)
            for rid, tc in text_chunks.items():
                report.update_text(rid, **tc.get_contents())

    def put_resource(self, key: Union[RunID, Tuple[str, str], str]):
        resource = self.get_resource(key)
        if isinstance(resource, ObjectID):
            address_manager.put((key, resource))
        if resource is not None:
            address = ray.put(resource)
            address_manager.put((key, address))

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



run_manager = RunManager()



class BaseFeatureConstructor(ABC):
    parallel = False
    cache = False
    worker = None

    def request_resource(self, key, df):
        if df._remote:
            request_time = time.time()
            rs.send(ResourceRequest(key))
            while not ray.get(address_manager.has.remote(key)) or ray.get(address_manager.timestamp.remote(key)) < request_time:
                time.sleep(0.01)
            address = ray.get(address_manager.get.remote(key))
            resource = ray.get(address)
            return resource
        else:
            rm = df.__meta__['run_manager']
            resource = rm.get_resource(key)
            if isinstance(resource, ObjectID):
                resource = ray.get(resource)
            return resource

    def sync(self, run_id, res_df, res_state, stats, df):
        if run_id.fold == "preview":
            return
        if not self.cache:
            res_df = None
        if df.__meta__['remote']:
            rs.send(Sync(run_id, res_df, res_state, stats))
        else:
            rm = df.__meta__['run_manager']
            rm.sync(run_id, res_df, res_state, stats)

    def __call__(self, df, ret=True):
        raise NotImplemented

    def get_alias(self, df):
        raise NotImplemented

    def remote_io(self):
        return redirect_stdout(RemoteTextIO())

    def local_io(self, report, run_id):
        return redirect_stdout(LocalTextIO(report, run_id))

    @contextmanager
    def set_scope(self, kf: KTSFrame, scope: str):
        tmp = kf.__meta__['scope']
        kf.__meta__['scope'] = scope
        yield
        kf.__meta__['scope'] = tmp

    def setup_worker(self):
        if self.worker is not None:
            return

        @ray.remote(num_return_vals=3)
        def worker(*args, df: pd.DataFrame, meta: Dict):
            kf = KTSFrame(df, meta=meta)
            kf.__meta__['remote'] = True
            had_state = bool(kf.state)
            stats = Stats(df)
            with stats:
                with self.remote_io():
                    res_kf = self.compute(*args, kf)
            if had_state:
                res_state = None
            else:
                res_state = res_kf._state
            return res_kf, res_state, stats.data

        self.worker = worker

    def local_worker(self, *args, kf: KTSFrame):
        run_id = RunID(kf._scope, kf._fold, kf.hash())
        had_state = bool(kf.state)
        stats = Stats(kf)
        if kf._remote:
            io = self.remote_io()
        else:
            io = self.local_io(kf.__meta__['report'], run_id)
        with stats:
            with io:
                res_kf = self.compute(*args, kf)
        if had_state:
            res_state = None
        else:
            res_state = res_kf._state
        return res_kf, res_state, stats.data

    def schedule(self, *args, scope: str, kf: KTSFrame) -> Tuple[RunID, Union[ObjectID, AnyFrame], Union[ObjectID, Dict], Union[ObjectID, Dict]]:
        run_id = RunID(scope, kf._fold, kf.hash())
        with self.set_scope(kf, scope):
            if self.parallel:
                self.setup_worker()
                meta = kf.__meta__
                oid = safe_put(kf)
                res_df, res_state, stats = self.worker.remote(*args, df=oid, meta=meta)
            else:                
                res_df, res_state, stats = self.local_worker(*args, kf=kf)
        return run_id, res_df, res_state, stats


class ParallelFeatureConstructor(BaseFeatureConstructor):
    parallel = True
    cache = True

    def wait(self, oids):
        """May be modified to emit ProgressSignals."""
        ray.wait(list(oids), len(oids))
        # for i in pbar(range(1, len(oids) + 1)):
        #     ray.wait(list(oids), i)

    def get_futures(self, kf: KTSFrame) -> Tuple[Dict[Tuple, ObjectID], Dict[Tuple, AnyFrame]]:
        scheduled_dfs = dict()
        result_dfs = dict()
        for args in self.map(kf):
            scope = self.get_scope(*args)
            run_id = RunID(scope, kf.__meta__['fold'], kf.hash())
            res_df = self.request_resource(run_id, kf)
            if res_df is not None:
                result_dfs[args] = res_df
                continue
            state = self.request_resource(run_id.state_id, kf)
            kf_arg = kf.clear_states()
            kf_arg.__meta__['remote'] = True
            if state is not None:
                kf_arg.set_scope(scope)
                kf_arg._state = state
            run_id, res_df, res_state, stats = self.schedule(*args, scope=scope, kf=kf_arg)
            self.sync(run_id, res_df, res_state, stats, kf)
            scheduled_dfs[args] = res_df
        return scheduled_dfs, result_dfs

    def assemble_futures(self, scheduled_dfs: Dict[Tuple, ObjectID], result_dfs: Dict[Tuple, AnyFrame], kf: KTSFrame) -> KTSFrame:
        for k, v in scheduled_dfs.items():
            result_dfs[k] = ray.get(v)
        res_list = list()
        for args in self.map(kf):
            res_list.append(result_dfs[args])
        res = self.reduce(res_list)
        res = KTSFrame(res)
        res.__meta__ = kf.__meta__
        return res

    def __call__(self, kf: KTSFrame, ret=True) -> Optional[KTSFrame]:
        scheduled_dfs, result_dfs = self.get_futures(kf)
        if not ret:
            return
        self.wait(scheduled_dfs.values())
        res = self.assemble_futures(scheduled_dfs, result_dfs, kf)
        return res

    def map(self, kf: KTSFrame):
        yield ()

    def compute(self, *args, kf: KTSFrame):
        raise NotImplemented

    def reduce(self, results: List[AnyFrame]) -> AnyFrame:
        return results[0]

    def get_scope(self, *args):
        return self.name

    def get_alias(self, kf: KTSFrame):
        assert self.cache
        fc = kf.__meta__['frame_cache']
        aliases = list()
        for args in self.map(kf):
            scope = self.get_scope(*args)
            run_id = RunID(scope, kf.__meta__['fold'], kf.hash())
            aliases.append(fc.load_run(run_id))
        res_alias = aliases[0]
        for alias in aliases[1:]:
            res_alias = res_alias.join(alias)
        return res_alias

class InlineFeatureConstructor(BaseFeatureConstructor):
    parallel = False
    cache = False

    def __call__(self, kf: KTSFrame, ret=False):
        return self.compute(kf, ret=ret)

    def get_alias(self, kf: KTSFrame):
        return self(kf)

class FeatureConstructor(ParallelFeatureConstructor):
    parallel = True
    cache = True

    def __init__(self, func):
        self.func = func
        self.name = func.__name__
        self.source = inspect.getsource(func)
        self.dependencies = self.extract_dependencies(func)

    def compute(self, kf: KTSFrame):
        kwargs = {key: self.request_resource(value, kf) for key, value in self.dependencies}
        result = self.func(kf, **kwargs)
        if '__columns' in kf._state and result.columns != kf._state['__columns']:
            fixed_columns = kf._state['__columns']
            for col in set(fixed_columns) - set(result.columns):
                result[col] = None
            return result[fixed_columns]
        if '__columns' not in kf._state:
            kf._state['__columns'] = list(kf.columns)
        return result

    def get_alias(self, kf: KTSFrame):
        run_id = RunID(self.name, kf.fold, kf.hash())
        rm = kf.__meta__['run_manager']
        return rm.get_run(run_id)

    def extract_dependencies(self, func):
        dependencies = dict()
        for k, v in inspect.signature(func).parameters.items():
            if isinstance(v.default, str) or isinstance(v.default, int) or isinstance(v.default, bool):
                dependencies[k] = v.default
            elif v.default != inspect._empty:
                raise UserWarning(f"Unknown argument: {k}={repr(v.default)}.")
        return dependencies


class GenericFeatureConstructor:
    def __init__(self, func, kwargs):
        warnings.warn('Generic feature support is still experimental, so please avoid corner cases.')
        self.func = func
        self.name = func.__name__
        self.source = inspect.getsource(func)
        self.parallel = kwargs.pop('parallel', True)
        self.cache = kwargs.pop('cache', True)
        self.kwargs = kwargs
        self.__call__ = forge.sign(**{arg: forge.arg(arg) for arg in self.kwargs})(self.__call__)

    def __call__(self, **kwargs):
        instance_kwargs = copy(self.kwargs)
        for k, v in kwargs.items():
            if k not in self.kwargs:
                raise ValueError(f"Unexpected arg: {k}")
            instance_kwargs[k] = v
        res = FeatureConstructor(self.modify(self.func, instance_kwargs))
        res.name = f"{self.name}_" + "_".join(map(str, instance_kwargs.values()))
        res.source = f"{self.name}({', '.join(f'{k}={repr(v)}' for k, v in instance_kwargs)})"
        res.parallel = self.parallel
        res.cache = self.cache
        return res

    def modify(self, func, instance_kwargs):
        @wraps(func)
        def new_func(*args, **kwargs):
            g = func.__globals__
            old_values = dict()
            for k, v in instance_kwargs.items():
                old_values[k] = g.get(k, None)
                g[k] = v

            try:
                res = func(*args, **kwargs)
            finally:
                for k, v in old_values.items():
                    if v is None:
                        del g[k]
                    else:
                        g[k] = v
            return res
        return new_func

