import time
from abc import ABC
from contextlib import redirect_stdout, contextmanager
from typing import Tuple, Union, Dict

import ray
from ray._raylet import ObjectID
from ray.experimental import signal as rs

from kts.core.backend.address_manager import get_address_manager
from kts.core.backend.io import RemoteTextIO, LocalTextIO
from kts.core.backend.signals import ResourceRequest, Sync
from kts.core.backend.stats import Stats
from kts.core.backend.util import safe_put
from kts.core.backend.worker import worker
from kts.core.cache import RunID, AnyFrame
from kts.core.frame import KTSFrame


class BaseFeatureConstructor(ABC):
    parallel = False
    cache = False
    worker = None

    def request_resource(self, key, df):
        if df._remote:
            request_time = time.time()
            rs.send(ResourceRequest(key))
            address_manager = get_address_manager()
            while not ray.get(address_manager.has.remote(key)): # TODO uncomment or ray.get(address_manager.timestamp.remote(key)) < request_time:
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
        # TODO remove
        # if run_id.fold == "preview":
        #     return
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

    # def setup_worker(self):
    #     if self.worker is not None:
    #         return

    #     @ray.remote(num_return_vals=3)
    #     def worker(*args, df: pd.DataFrame, meta: Dict):
    #         kf = KTSFrame(df, meta=meta)
    #         kf.__meta__['remote'] = True
    #         had_state = bool(kf.state)
    #         stats = Stats(df)
    #         with stats:
    #             with self.remote_io():
    #                 res_kf = self.compute(*args, kf)
    #         if had_state:
    #             res_state = None
    #         else:
    #             res_state = res_kf._state
    #         return res_kf, res_state, stats.data

    #     self.worker = worker

    def local_worker(self, *args, kf: KTSFrame):
        run_id = RunID(kf._scope, kf._fold, kf.hash())
        had_state = bool(kf.state)
        stats = Stats(kf)
        if kf._remote:
            io = self.remote_io()
        # else:
        #     io = self.local_io(kf.__meta__['report'], run_id)
        with stats:
            # with io:
            res_kf = self.compute(*args, kf)
        if had_state:
            res_state = None
        else:
            res_state = kf._state
        return res_kf, res_state, stats.data

    def schedule(self, *args, scope: str, kf: KTSFrame) -> Tuple[RunID, Union[ObjectID, AnyFrame], Union[ObjectID, Dict], Union[ObjectID, Dict]]:
        run_id = RunID(scope, kf._fold, kf.hash())
        with self.set_scope(kf, scope):
            if self.parallel:
                # self.setup_worker()
                meta = kf.__meta__
                oid = safe_put(kf)
                res_df, res_state, stats = worker.remote(self, *args, df=oid, meta=meta)
            else:
                res_df, res_state, stats = self.local_worker(*args, kf=kf)
        return run_id, res_df, res_state, stats
