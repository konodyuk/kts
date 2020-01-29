import pprint
from typing import Union, List, Tuple, Optional, Collection

import numpy as np
import pandas as pd

from kts.core import ui
from kts.core.cache import frame_cache
from kts.core.frame import KTSFrame
from kts.core.lists import feature_list
from kts.core.runtime import FeatureConstructor
from kts.core.runtime import run_manager
from kts.settings import cfg
from kts.util.hashing import hash_list, hash_fold

AnyFrame = Union[pd.DataFrame, KTSFrame]


class PreviewDataFrame:
    """Looks like a real DataFrame without exposing its data to the global namespace.
    
    Prevents memory leaks when returning a new DataFrame object.
    To be clear, the leak is caused not by just returning a new dataframe, 
    but by saving it to IPython variable _{cell_run_id} (_35).
    Therefore, the provided example will work only in IPython, but not in regular python console.
    
    Used in @preview and some of the methods of FeatureSet.
    
    Example:
    ```
    big_df = pd.DataFrame(np.random.rand(1000000, 100), columns=range(100))

    def ret_slice():
        return big_df.loc[np.arange(100, 1000000, 2), [2, 3]]
    ret_slice() # run 10-20 times -- memory leak (+10 mb each time)

    def ret_slice_preview():
        return PreviewDataFrame(big_df.loc[np.arange(100, 1000000, 2), [2, 3]])
    ret_slice_preview() # no leak
    ```
    """
    def __init__(self, dataframe):
        self.repr_html = dataframe._repr_html_()
        self.repr_plaintext = dataframe.__repr__()
    
    def _repr_html_(self):
        """HTML repr used in notebooks."""
        return self.repr_html
    
    def __repr__(self):
        """Plaintext repr used in console."""
        return self.repr_plaintext


class FeatureSet:
    def __init__(self,
                 before_split: List[FeatureConstructor],
                 after_split: Optional[List[FeatureConstructor]] = None,
                 # overfitted: Optional[List[FeatureConstructor]] = None, # TODO
                 train_frame: AnyFrame = None,
                 test_frame: AnyFrame = None,
                 targets: Optional[Union[List[str], str]] = None,
                 auxiliary: Optional[Union[List[str], str]] = None,
                 description: Optional[str] = None):
        self.before_split = before_split
        self.after_split = after_split
        if not bool(self.before_split):
            self.before_split = []
        if not bool(self.after_split):
            self.after_split = []
        # self.overfitted = overfitted
        self.train_frame = frame_cache.save(train_frame)
        if test_frame is not None:
            self.test_frame = frame_cache.save(test_frame)
        if isinstance(targets, str):
            targets = [targets]
        self.targets = targets
        if isinstance(auxiliary, str):
            auxiliary = [auxiliary]
        self.auxiliary = auxiliary
        self.description = description

    def split(self, folds):
        return CVFeatureSet(self, folds)

    def compute(self, frame=None, report=None):
        if report is None:
            report = ui.FeatureComputingReport(feature_list)
        if frame is None:
            frame = frame_cache.load(self.train_frame)
        frame = KTSFrame(frame)
        frame.__meta__['fold'] = '0000'
        frame.__meta__['train'] = True
        parallel_cache = [i for i in self.before_split if i.parallel and i.cache]
        run_manager.run(parallel_cache, frame, remote=True)
        run_manager.supervise(report)
        not_parallel_cache = [i for i in self.before_split if not i.parallel and i.cache]
        run_manager.run(not_parallel_cache, frame, remote=False)
        run_manager.merge_scheduled()

    @property
    def target(self) -> Union[PreviewDataFrame, AnyFrame]:
        res = frame_cache.load(self.train_frame, columns=self.targets)
        if cfg.preview_mode:
            return PreviewDataFrame(res)
        else:
            return res

    @property
    def aux(self) -> Union[PreviewDataFrame, AnyFrame]:
        res = frame_cache.load(self.train_frame, columns=self.auxiliary)
        if cfg.preview_mode:
            return PreviewDataFrame(res)
        else:
            return res

    def __getitem__(self, key) -> PreviewDataFrame:
        raise NotImplemented

    @property
    def source(self):
        return f"FeatureSet({pprint.pformat([i.name for i in self.before_split])},\n " \
               f"{pprint.pformat([i.name for i in self.after_split])})"

    @property
    def features(self):
        return self.before_split + self.after_split

    @property
    def feature_pool(self):
        return ui.Pool([ui.Raw(i.html_collapsible()) for i in self.features])
    
    def _html_elements(self, include_features=True):
        elements = [ui.Annotation('name'), ui.Field(self.name)]
        if self.description is not None:
            elements += [ui.Annotation('description'), ui.Field(self.description)]
        if include_features:
            elements += [ui.Annotation('features'), self.feature_pool]
        elements += [ui.Annotation('source'), ui.Code(self.source)]
        return elements
    
    @property
    def html(self):
        return ui.Column([ui.Title('feature set')] + self._html_elements()).html
    
    @property
    def html_collapsible(self):
        cssid = np.random.randint(1000000000)
        elements = [ui.TitleWithCross('feature set', cssid)]
        elements += self._html_elements(include_features=False)
        return ui.CollapsibleColumn(elements, ui.ThumbnailField('feature set', cssid), cssid).html

    @property
    def name(self):
        sources_before = [i.source for i in self.before_split]
        sources_after = [i.source for i in self.after_split]
        return f"FS{hash_list(sources_before, 2)}{hash_list(sources_after, 2)}"
    


class CVFeatureSet:
    def __init__(self, feature_set: FeatureSet, folds: List[Tuple[Collection, Collection]]):
        self.feature_set = feature_set
        self.fold_ids = []
        for idx_train, idx_valid in folds:
            assert len(set(idx_train) & set(idx_valid)) == 0 or all(idx_train == idx_valid), \
            "Train and valid sets should either not intersect or be equal"
            self.fold_ids.append(hash_fold(idx_train, idx_valid))
        self.folds = folds

    def compute(self, frame: AnyFrame = None, report=None):
        self.feature_set.compute()
        if report is None:
            report = ui.FeatureComputingReport(feature_list)
        if frame is not None:
            for i in range(self.n_folds):
                fold = self.fold(i)
                fold.compute(frame, parallel=True, cache=True, report=report)
            for i in range(self.n_folds):
                fold = self.fold(i)
                fold.compute(frame, parallel=False, cache=True, report=report)
            return
        for i in range(self.n_folds):
            fold = self.fold(i)
            fold.compute(fold.train_frame, parallel=True, cache=True, report=True, train=True)
        for i in range(self.n_folds):
            fold = self.fold(i)
            fold.compute(fold.valid_frame, parallel=True, cache=True, report=True, train=False)
        for i in range(self.n_folds):
            fold = self.fold(i)
            fold.compute(fold.train_frame, parallel=False, cache=True, report=True, train=True)
        for i in range(self.n_folds):
            fold = self.fold(i)
            fold.compute(fold.valid_frame, parallel=False, cache=True, report=True, train=False)

    # def compute_parallel(self, frame, report):
    #     parallel_cache = [i for i in self.after_split if i.parallel and i.cache]
    #     run_manager.run(parallel_cache, frame, remote=True)
    #     run_manager.supervise(report)

    # def compute_not_parallel(self, frame, report):
    #     not_parallel_cache = [i for i in self.after_split if not i.parallel and i.cache]
    #     run_manager.run(not_parallel_cache, frame, remote=False)

    @property
    def computed(self) -> bool:
        raise NotImplemented

    @property
    def train_frame(self):
        return self.feature_set.train_frame

    @property
    def n_folds(self):
        return len(self.folds)
    
    def fold(self, idx) -> Fold:
        return Fold(self, idx)


class Fold:
    def __init__(self, cv_feature_set: CVFeatureSet, fold_idx: int):
        self.cv_feature_set = cv_feature_set
        self.fold_idx = fold_idx

    def compute(self, frame, report=None, parallel=None, cache=None, before=False, train=False):
        frame = KTSFrame(frame)
        frame.__meta__['fold'] = self.fold_id
        frame.__meta__['train'] = train
        if before:
            feature_constructors = self.before_split
        else:
            feature_constructors = self.after_split
        queue = [i for i in feature_constructors if i.parallel == parallel and i.cache == cache]
        return run_manager.run(queue, frame, remote=parallel, ret=(not cache), report=report)
        
    @property
    def train(self) -> np.ndarray:
        self.cv_feature_set.compute()
        frames = dict()
        frames.update(self.aliases_before(self.train_frame, train=True))
        frames.update(self.aliases_after(self.train_frame, train=True))
        frames.update(self.compute(self.train_frame, report=self.report, parallel=True, cache=False, before=True, train=True))
        frames.update(self.compute(self.train_frame, report=self.report, parallel=True, cache=False, before=False, train=True))
        frames.update(self.compute(self.train_frame, report=self.report, parallel=False, cache=False, before=True, train=True))
        frames.update(self.compute(self.train_frame, report=self.report, parallel=False, cache=False, before=False, train=True))
        frames_sorted = [frames[fc.name] for fc in self.before_split + self.after_split]
        return frame_cache.concat(frames_sorted)

    @property
    def valid(self) -> np.ndarray:
        self.cv_feature_set.compute()
        frames = dict()
        frames.update(self.aliases_before(self.valid_frame))
        frames.update(self.aliases_after(self.valid_frame))
        frames.update(self.compute(self.valid_frame, report=self.report, parallel=True, cache=False, before=True))
        frames.update(self.compute(self.valid_frame, report=self.report, parallel=True, cache=False, before=False))
        frames.update(self.compute(self.valid_frame, report=self.report, parallel=False, cache=False, before=True))
        frames.update(self.compute(self.valid_frame, report=self.report, parallel=False, cache=False, before=False))
        frames_sorted = [frames[fc.name] for fc in self.before_split + self.after_split]
        return frame_cache.concat(frames_sorted)

    def __call__(self, frame: AnyFrame) -> np.ndarray:
        self.cv_feature_set.compute()
        frames = dict()
        frames.update(self.aliases_before(frame))
        frames.update(self.aliases_after(frame))
        frames.update(self.compute(frame, report=self.report, parallel=True, cache=False, before=True))
        frames.update(self.compute(frame, report=self.report, parallel=True, cache=False, before=False))
        frames.update(self.compute(frame, report=self.report, parallel=False, cache=False, before=True))
        frames.update(self.compute(frame, report=self.report, parallel=False, cache=False, before=False))
        frames_sorted = [frames[fc.name] for fc in self.before_split + self.after_split]
        return frame_cache.concat(frames_sorted)

    @property
    def train_target(self) -> np.ndarray:
        self.cv_feature_set.compute()
        return self.alias_before(train=True) & self.targets

    @property
    def valid_target(self) -> np.ndarray:
        self.cv_feature_set.compute()
        return self.alias_before(train=False) & self.targets

    def aliases_before(self, frame: AnyFrame, train=False):
        frame = KTSFrame(frame)
        frame.__meta__['fold'] = self.fold_id
        frame.__meta__['train'] = train
        return [i.get_alias(frame) for i in self.before_split if i.cache]

    def alias_before(self, train=False):
        frame = self.train_frame if train else self.valid_frame
        aliases = self.aliases_before(frame, train=train)
        res_alias = aliases[0]
        for alias in aliases[1:]:
            res_alias = res_alias.join(alias)
        return res_alias

    def aliases_after(self, frame: AnyFrame, train=False):
        frame = KTSFrame(frame)
        frame.__meta__['fold'] = self.fold_id
        frame.__meta__['train'] = train
        return [i.get_alias(frame) for i in self.after_split if i.cache]
    
    @property
    def before_split(self):
        return self.feature_set.before_split

    @property
    def after_split(self):
        return self.feature_set.after_split

    @property
    def feature_set(self):
        return self.cv_feature_set.feature_set
    
    @property
    def train_frame(self):
        name = self.feature_set.train_frame
        return frame_cache.load(name, index=self.idx_train)

    @property
    def valid_frame(self):
        name = self.feature_set.train_frame
        return frame_cache.load(name, index=self.idx_train)

    @property
    def idx_train(self):
        return self.cv_feature_set.folds[self.fold_idx][0]

    @property
    def idx_valid(self):
        return self.cv_feature_set.folds[self.fold_idx][1]

    @property
    def fold_id(self):
        return self.cv_feature_set.fold_ids[self.fold_idx]
    
    @property
    def report(self):
        return self.cv_feature_set.report if 'report' in dir(self.cv_feature_set) else None

    @property
    def targets(self):
        return self.feature_set.targets
