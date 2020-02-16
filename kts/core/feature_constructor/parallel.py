from typing import Tuple, Dict, Optional, List

import ray
from ray._raylet import ObjectID

from kts.core.cache import AnyFrame, RunID
from kts.core.feature_constructor.base import BaseFeatureConstructor
from kts.core.frame import KTSFrame


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
            # kf_arg.__meta__['remote'] = True
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
        for args in self.map(kf):
            res_list.append(result_dfs[args])
        res = self.reduce(res_list)
        # res = KTSFrame(res)
        # res.__meta__ = kf.__meta__
        return res

    def __call__(self, kf: KTSFrame, ret=True) -> Optional[KTSFrame]:
        scheduled_dfs, result_dfs = self.get_futures(kf)
        if not ret:
            return
        if 'run_manager' in kf.__meta__:
            rm = kf.__meta__['run_manager']
            rm.supervise()
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
