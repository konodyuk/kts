import time
from abc import ABC
from contextlib import redirect_stdout, contextmanager, redirect_stderr
from io import StringIO
from typing import Tuple, Union, Dict, List

import numpy as np
import ray
from ray._raylet import ObjectID

import kts.core.backend.signal as rs
import kts.ui.components as ui
from kts.core.backend.address_manager import get_address_manager
from kts.core.backend.io import RemoteTextIO, LocalTextIO, SuppressIO
from kts.core.backend.progress import ProgressSignal
from kts.core.backend.progress import pbar
from kts.core.backend.signal import Sync, ResourceRequest
from kts.core.backend.stats import Stats
from kts.core.backend.util import in_worker
from kts.core.backend.util import safe_put
from kts.core.backend.worker import worker
from kts.core.frame import KTSFrame
from kts.core.run_id import RunID
from kts.core.types import AnyFrame


class BaseFeatureConstructor(ABC, ui.HTMLRepr):
    parallel = False
    cache = False
    worker = None
    registered = False
    source = None
    additional_source = None
    requirements = None
    description = None
    columns = None
    verbose = False

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
            # result frame is not saved to cache if FC is not cached
            # refer to https://github.com/konodyuk/kts/tree/master/kts/core#caching-policy
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
        raise NotImplementedError

    def remote_io(self, run_id=None):
        return redirect_stdout(RemoteTextIO(run_id))

    def local_io(self, report, run_id):
        return redirect_stdout(LocalTextIO(report, run_id))

    def suppress_io(self):
        return redirect_stdout(SuppressIO())

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
        return_state = kf._train  # default for cached FCs or first calls of not cached FCs
        if not self.cache and bool(kf._state):
            # second call of not cached FC does not return state, as it is saved previously
            # refer to https://github.com/konodyuk/kts/tree/master/kts/core#caching-policy
            return_state = False
        stats = Stats(kf)
        if in_worker() and self.verbose:
            report = None
            io = self.remote_io(run_id)
            rs.send(ProgressSignal(0, 1, None, None, None, run_id))
        elif not in_worker() and self.verbose:
            report = kf.__meta__['report']
            io = self.local_io(report, run_id)
            report.update(run_id, 0, 1)
        else:
            report = None
            io = self.suppress_io()
        with stats, io, self.suppress_stderr(), pbar.local_mode(report, run_id):
            res_kf = self.compute(*args, kf)

        if 'columns' in dir(res_kf) and '__columns' not in kf._state:
            kf._state['__columns'] = list(res_kf.columns)

        if return_state:
            res_state = kf._state
        else:
            res_state = None
        if in_worker() and self.verbose:
            rs.send(ProgressSignal(1, 1, stats.data['took'], None, None, run_id))
        elif not in_worker() and self.verbose:
            report = kf.__meta__['report']
            report.update(run_id, 1, 1, stats.data['took'])
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
        if not self.registered:
            return self.source
        return self.name

    def _html_elements(self):
        elements = [ui.Annotation('name'), ui.Field(repr(self))]
        if self.description is not None:
            elements += [ui.Annotation('description'), ui.Field(self.description)]
        elements += [ui.Annotation('source'), ui.Code(self.source)]
        if self.additional_source:
            elements += [ui.Annotation('additional source'), ui.Code(self.additional_source)]
        if self.columns:
            elements += [ui.Annotation('columns'), ui.Field('<tt>' + ', '.join(self.columns) + '</tt>')]
        if self.requirements:
            elements += [ui.Annotation('requirements'), ui.Field('<tt>' + ', '.join(self.requirements) + '</tt>')]
        # if self.preview_df is not None:
        #     elements += [ui.Annotation('preview'), ui.DF(self.preview_df)]
        return elements

    @property
    def html(self):
        elements = [ui.Title('feature constructor')]
        elements += self._html_elements()
        return ui.Column(elements).html

    def html_collapsible(self, name=None, style="", border=False, **kw):
        if name is None:
            name = repr(self)
        css_id = np.random.randint(1000000000)
        elements = [ui.TitleWithCross('feature constructor', css_id)]
        elements += self._html_elements()
        thumbnail = ui.ThumbnailField(name, css_id, style=style, **kw)
        return ui.CollapsibleColumn(elements, thumbnail, css_id, border=border).html

    def __and__(self, columns: List[str]):
        return Selector(self, columns=columns)

    def __sub__(self, columns: List[str]):
        return Dropper(self, columns=columns)


class InlineFeatureConstructor(BaseFeatureConstructor):
    parallel = False
    cache = False

    def __call__(self, kf: KTSFrame, ret=True):
        return self.compute(kf, ret=ret)


class Selector(InlineFeatureConstructor):
    def __init__(self, feature_constructor, columns):
        self.feature_constructor = feature_constructor
        self.selected_columns = columns
        self.name = f"{feature_constructor.name}_select_" + '_'.join(columns)

    def compute(self, kf: KTSFrame, ret=True):
        res = self.feature_constructor(kf, ret=ret)
        if ret:
            to_select = res.columns.intersection(self.selected_columns)
            return res[to_select]

    @property
    def cache(self):
        return self.feature_constructor.cache

    @property
    def parallel(self):
        return self.feature_constructor.parallel

    @property
    def source(self):
        column_intersection = set(self.selected_columns)
        if self.feature_constructor.columns is not None:
            column_intersection &= set(self.feature_constructor.columns)
        column_intersection = list(column_intersection)
        return f"{repr(self.feature_constructor)} & {column_intersection}"

    @property
    def additional_source(self):
        return self.feature_constructor.source

    @property
    def columns(self):
        column_intersection = set(self.selected_columns)
        if self.feature_constructor.columns is not None:
            column_intersection &= set(self.feature_constructor.columns)
        column_intersection = list(column_intersection)
        return column_intersection

    def __and__(self, columns: List[str]):
        return Selector(self.feature_constructor, columns=set(self.columns) & set(columns))

    def __sub__(self, columns: List[str]):
        return Selector(self.feature_constructor, columns=set(self.columns) - set(columns))


class Dropper(InlineFeatureConstructor):
    def __init__(self, feature_constructor, columns):
        self.feature_constructor = feature_constructor
        self.dropped_columns = columns
        self.name = f"{feature_constructor.name}_drop_" + '_'.join(columns)

    def compute(self, kf: KTSFrame, ret=True):
        res = self.feature_constructor(kf, ret=ret)
        if ret:
            to_drop = res.columns.intersection(self.dropped_columns)
            return res.drop(to_drop, axis=1)

    @property
    def cache(self):
        return self.feature_constructor.cache

    @property
    def parallel(self):
        return self.feature_constructor.parallel

    @property
    def source(self):
        column_intersection = set(self.dropped_columns)
        if self.feature_constructor.columns is not None:
            column_intersection &= set(self.feature_constructor.columns)
        column_intersection = list(column_intersection)
        return f"{repr(self.feature_constructor)} - {column_intersection}"

    @property
    def additional_source(self):
        return self.feature_constructor.source

    @property
    def columns(self):
        if self.feature_constructor.columns is None:
            return list()
        return list(set(self.feature_constructor.columns) - set(self.dropped_columns))

    def __and__(self, columns: List[str]):
        return Selector(self.feature_constructor, columns=set(self.columns) & set(columns))

    def __sub__(self, columns: List[str]):
        return Dropper(self.feature_constructor, columns=set(self.dropped_columns) | set(columns))
