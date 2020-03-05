import pprint
from typing import Union, List, Tuple, Optional, Collection

import numpy as np
import pandas as pd

import kts.ui.components as ui
from kts.core.backend.run_manager import run_manager
from kts.core.feature_constructor.base import BaseFeatureConstructor
from kts.core.feature_constructor.user_defined import FeatureConstructor
from kts.core.frame import KTSFrame
from kts.core.lists import feature_list
from kts.settings import cfg
from kts.ui.feature_computing_report import SilentFeatureComputingReport
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


class FeatureSet(ui.HTMLRepr):
    def __init__(self,
                 before_split: List[FeatureConstructor],
                 after_split: Optional[List[FeatureConstructor]] = None,
                 # overfitted: Optional[List[FeatureConstructor]] = None, # TODO
                 train_frame: AnyFrame = None,
                 test_frame: AnyFrame = None,
                 targets: Optional[Union[List[str], str]] = None,
                 auxiliary: Optional[Union[List[str], str]] = None,
                 description: Optional[str] = None):
        self.before_split, self.after_split = self.check_features(before_split, after_split)
        self.train_frame = train_frame
        self.test_frame = test_frame
        self.targets = self.check_columns(targets)
        self.auxiliary = self.check_columns(auxiliary)
        self.description = description

    def check_features(self, before, after):
        if before is None:
            before = []
        if after is None:
            after = []
        for feature in before + after:
            assert isinstance(feature, BaseFeatureConstructor), f"{feature.name} is not a FeatureConstructor"
            assert not (feature.parallel and not feature.cache), f"{feature.name} cannot be parallel but not cached"
        for i, feature in enumerate(before):
            if not feature.cache:
                after.append(before.pop(i))
        return before, after

    def check_columns(self, columns):
        if columns is None:
            return []
        if not isinstance(columns, list):
            columns = [columns]
        for column in columns:
            assert isinstance(column, str), f"Column names should be of type str"
        return columns

    def split(self, folds):
        return CVFeatureSet(self, folds)

    def compute(self, frame=None, report=None):
        if report is None:
            report = SilentFeatureComputingReport()
        if frame is None:
            frame = self.train_frame
            train = True
        else:
            train = False
        parallel = [i for i in self.before_split if i.parallel]
        run_manager.run(parallel, frame, train=train, ret=False, report=report)
        run_manager.supervise(report)
        not_parallel = [i for i in self.before_split if not i.parallel]
        run_manager.run(not_parallel, frame, train=train, ret=False, report=report)
        run_manager.merge_scheduled()

    @property
    def target(self) -> Union[PreviewDataFrame, AnyFrame]:
        frame = self.train_frame
        self.compute()
        target_fcs = [fc for fc in self.before_split if len(set(self.targets) & set(fc.columns))]
        results = list(run_manager.run(target_fcs, frame, train=True, ret=True).values())
        results.append(frame)
        for i, frame in enumerate(results):
            results[i] = frame[frame.columns.intersection(self.targets)]
        res = concat(results)
        if cfg.preview_mode:
            return PreviewDataFrame(res)
        else:
            return res

    @property
    def aux(self) -> Union[PreviewDataFrame, AnyFrame]:
        frame = self.train_frame
        self.compute()
        aux_fcs = [fc for fc in self.before_split if len(set(self.auxiliary) & set(fc.columns))]
        results = list(run_manager.run(aux_fcs, frame, train=True, ret=True).values())
        results.append(frame)
        for i, frame in enumerate(results):
            results[i] = frame[frame.columns.intersection(self.auxiliary)]
        res = concat(results)
        if cfg.preview_mode:
            return PreviewDataFrame(res)
        else:
            return res

    def __getitem__(self, key) -> PreviewDataFrame:
        raise NotImplemented

    @property
    def source(self):
        res =  "FeatureSet("
        shift = ' ' * len(res)
        res += '[' + (',\n' + shift + ' ').join([repr(i) for i in self.before_split]) + '],\n'
        res += shift
        res += '[' + (',\n' + shift + ' ').join([repr(i) for i in self.after_split]) + '],\n'
        res += f"{shift}targets={pprint.pformat(self.targets)},\n"
        res += f"{shift}auxiliary={pprint.pformat(self.auxiliary)})"
        return res

    @property
    def features(self):
        return self.before_split + self.after_split

    @property
    def feature_pool(self):
        elements = list()
        for i in self.features:
            name = repr(i)
            if '(' in name:
                name = name[:name.find('(')]
            elements.append(ui.Raw(i.html_collapsible(name=name)))
        return ui.Pool(elements)

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
        css_id = np.random.randint(1000000000)
        elements = [ui.TitleWithCross('feature set', css_id)]
        elements += self._html_elements(include_features=False)
        return ui.CollapsibleColumn(elements, ui.ThumbnailField('feature set', css_id), css_id).html

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
        if report is None:
            report = SilentFeatureComputingReport(feature_list)
        self.feature_set.compute(frame=frame, report=report)
        for i in range(self.n_folds):
            self.fold(i).compute(frame=frame, report=report, train=True)
            self.fold(i).compute(frame=frame, report=report, train=False)
        run_manager.merge_scheduled()

    @property
    def computed(self) -> bool:
        raise NotImplemented

    @property
    def train_frame(self):
        return self.feature_set.train_frame

    @property
    def n_folds(self):
        return len(self.folds)
    
    def fold(self, idx) -> 'Fold':
        return Fold(self, idx)


class Fold:
    def __init__(self, cv_feature_set: CVFeatureSet, fold_idx: int):
        self.cv_feature_set = cv_feature_set
        self.fold_idx = fold_idx

    def compute(self, frame=None, report=None, train=None):
        if report is None:
            report = SilentFeatureComputingReport()
        if frame is None:
            assert train is not None
            if train:
                frame = self.train_frame
            else:
                frame = self.valid_frame
        else:
            train = False
        cached = [i for i in self.after_split if i.cache]
        parallel = [i for i in cached if i.parallel]
        run_manager.run(parallel, frame, train=train, fold=self.fold_id, ret=False, report=report)
        run_manager.supervise(report)
        not_parallel = [i for i in cached if not i.parallel]
        run_manager.run(not_parallel, frame, train=train, fold=self.fold_id, ret=False, report=report)

    @property
    def train(self) -> np.ndarray:
        results = dict()
        frame = self.feature_set.train_frame
        results.update(run_manager.run(self.before_split, frame, train=True, ret=True))
        frame = self.train_frame
        results.update(run_manager.run(self.after_split, frame, train=True, fold=self.fold_id, ret=True))
        results.update({'_': self.train_frame[[]]})
        return concat(results.values()).values

    @property
    def valid(self) -> np.ndarray:
        results = dict()
        frame = self.feature_set.train_frame
        results.update(run_manager.run(self.before_split, frame, train=True, ret=True))
        frame = self.valid_frame
        results.update(run_manager.run(self.after_split, frame, train=False, fold=self.fold_id, ret=True))
        results.update({'_': self.valid_frame[[]]})
        return concat(results.values()).values

    def __call__(self, frame: AnyFrame) -> np.ndarray:
        results = dict()
        results.update(run_manager.run(self.before_split, frame, train=False, ret=True))
        results.update(run_manager.run(self.after_split, frame, train=False, fold=self.fold_id, ret=True))
        return concat(results.values()).values

    @property
    def train_target(self) -> np.ndarray:
        return self.feature_set.target.iloc[self.idx_train].values

    @property
    def valid_target(self) -> np.ndarray:
        return self.feature_set.target.iloc[self.idx_valid].values
    
    @property
    def before_split(self):
        return self.feature_set.before_split

    @property
    def after_split(self):
        return self.feature_set.after_split

    @property
    def features(self):
        return self.before_split + self.after_split

    @property
    def feature_set(self):
        return self.cv_feature_set.feature_set
    
    @property
    def train_frame(self):
        return self.feature_set.train_frame.iloc[self.idx_train]

    @property
    def valid_frame(self):
        return self.feature_set.train_frame.iloc[self.idx_valid]

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
    def targets(self):
        return self.feature_set.targets


def concat(frames: List[pd.DataFrame]):
    frames = list(frames)
    common_index = frames[0].index
    for frame in frames:
        common_index = common_index.intersection(frame.index)
    for i, frame in enumerate(frames):
        if len(frame.index) != len(common_index) or not all(frame.index == common_index):
            frames[i] = frame.loc[common_index]
    return pd.concat(frames, axis=1)
