from typing import Tuple, Dict, Optional, List

import ray
from ray._raylet import ObjectID

from kts.core.backend.run_manager import run_cache
from kts.core.feature_constructor.base import BaseFeatureConstructor
from kts.core.frame import KTSFrame
from kts.core.run_id import RunID
from kts.core.types import AnyFrame


class ParallelFeatureConstructor(BaseFeatureConstructor):
    parallel = True
    cache = True

    def wait(self, oids):
        """May be modified to emit ProgressSignals."""
        if len(oids) == 0:
            return
        ray.wait(list(oids), len(oids))
        # for i in pbar(range(1, len(oids) + 1)):
        #     ray.wait(list(oids), i)

    def get_futures(self, kf: KTSFrame) -> Tuple[Dict[Tuple, ObjectID], Dict[Tuple, AnyFrame]]:
        scheduled_dfs = dict()
        result_dfs = dict()
        for args in self.split(kf):
            scope = self.get_scope(*args)
            run_id = RunID(scope, kf._fold, kf.hash())
            res_df = self.request_resource(run_id, kf)
            if res_df is not None:
                result_dfs[args] = res_df
                continue
            state = self.request_resource(run_id.state_id, kf)
            if self.parallel:
                kf_arg = kf.clear_states()
            else:
                kf_arg = kf
            if state is not None:
                kf_arg.set_scope(scope)
                kf_arg.__states__[kf_arg._state_key] = state
            run_id, res_df, res_state, stats = self.schedule(*args, scope=scope, kf=kf_arg)
            self.sync(run_id, res_df, res_state, stats, kf)
            if self.parallel:
                scheduled_dfs[args] = res_df
            else:
                result_dfs[args] = res_df
        return scheduled_dfs, result_dfs

    def assemble_futures(self, scheduled_dfs: Dict[Tuple, ObjectID], result_dfs: Dict[Tuple, AnyFrame], kf: KTSFrame) -> KTSFrame:
        for k, v in scheduled_dfs.items():
            result_dfs[k] = ray.get(v)
        res_list = list()
        for args in self.split(kf):
            res_list.append(result_dfs[args])
        res = self.reduce(res_list)
        res = KTSFrame(res)
        res.__meta__ = kf.__meta__
        return res

    def __call__(self, kf: KTSFrame, ret=True) -> Optional[KTSFrame]:
        scheduled_dfs, result_dfs = self.get_futures(kf)
        if not ret:
            return
        self.supervise(kf)
        self.wait(scheduled_dfs.values())
        res = self.assemble_futures(scheduled_dfs, result_dfs, kf)
        return res

    def split(self, kf: KTSFrame):
        yield ()

    def compute(self, *args, kf: KTSFrame):
        raise NotImplementedError

    def reduce(self, results: List[AnyFrame]) -> AnyFrame:
        return results[0]

    def get_scope(self, *args):
        return self.name

    @property
    def columns(self):
        try:
            result = set()
            for args in self.split(None):
                result |= set(run_cache.get_columns(self.get_scope(*args)))
            return result
        except:
            return None
