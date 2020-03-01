import time
from abc import ABC
from contextlib import redirect_stdout, contextmanager, redirect_stderr
from io import StringIO
from typing import Tuple, Union, Dict

import ray
from ray._raylet import ObjectID
from ray.experimental import signal as rs

from kts.core.backend.address_manager import get_address_manager
from kts.core.backend.io import RemoteTextIO, LocalTextIO
from kts.core.backend.progress import pbar
from kts.core.backend.signals import ResourceRequest, Sync
from kts.core.backend.stats import Stats
from kts.core.backend.util import in_worker
from kts.core.backend.util import safe_put
from kts.core.backend.worker import worker
from kts.core.frame import KTSFrame
from kts.core.run_id import RunID
from kts.core.types import AnyFrame


class BaseFeatureConstructor(ABC):
    parallel = False
    cache = False
    worker = None
    registered = False

    def request_resource(self, key, df):
        if in_worker():
            request_time = time.time()
            rs.send(ResourceRequest(key))
            address_manager = get_address_manager()
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
        if not self.cache:
            res_df = None
        if in_worker():
            if not isinstance(res_df, ObjectID):
                res_df = ray.put(res_df)
            if not isinstance(res_state, ObjectID):
                res_state = ray.put(res_state)
            if not isinstance(stats, ObjectID):
                stats = ray.put(stats)
            rs.send(Sync(run_id, res_df, res_state, stats))
        else:
            rm = df.__meta__['run_manager']
            rm.sync(run_id, res_df, res_state, stats)

    def __call__(self, df, ret=True):
        raise NotImplemented

    def remote_io(self):
        return redirect_stdout(RemoteTextIO())

    def local_io(self, report, run_id):
        return redirect_stdout(LocalTextIO(report, run_id))

    def suppress_stderr(self):
        return redirect_stderr(StringIO())

    @contextmanager
    def set_scope(self, kf: KTSFrame, scope: str):
        tmp = kf.__meta__['scope']
        kf.__meta__['scope'] = scope
        yield
        kf.__meta__['scope'] = tmp

    def local_worker(self, *args, kf: KTSFrame):
        run_id = RunID(kf._scope, kf._fold, kf.hash())
        had_state = bool(kf.state)
        stats = Stats(kf)
        if in_worker():
            report = None
            io = self.remote_io()
        else:
            report = kf.__meta__['report']
            io = self.local_io(report, run_id)
            report.update(run_id, 0, 1)
        with stats, io, self.suppress_stderr(), pbar.local_mode(report, run_id):
            res_kf = self.compute(*args, kf)
        if had_state:
            res_state = None
        else:
            res_state = kf._state
        if not in_worker():
            report = kf.__meta__['report']
            report.update(run_id, 1, 1)
        return res_kf, res_state, stats.data

    def schedule(self, *args, scope: str, kf: KTSFrame) -> Tuple[RunID, Union[ObjectID, AnyFrame], Union[ObjectID, Dict], Union[ObjectID, Dict]]:
        run_id = RunID(scope, kf._fold, kf.hash())
        with self.set_scope(kf, scope):
            if self.parallel:
                meta = kf.__meta__
                oid = safe_put(kf)
                res_df, res_state, stats = worker.remote(self, *args, df=oid, meta=meta)
            else:
                res_df, res_state, stats = self.local_worker(*args, kf=kf)
        return run_id, res_df, res_state, stats

    def supervise(self, kf: KTSFrame):
        if 'run_manager' in kf.__meta__:
            rm = kf.__meta__['run_manager']
            report = kf.__meta__['report']
            rm.supervise(report=report)

    def __repr__(self):
        if self.registered:
            return self.name
        else:
            return self.source
