import pprint
from collections import defaultdict
from copy import copy
from typing import Union, List, Tuple, Optional, Collection, Set

import numpy as np
import pandas as pd

import kts.ui.components as ui
from kts.core.backend.run_manager import run_manager
from kts.core.feature_constructor.base import BaseFeatureConstructor
from kts.core.feature_constructor.user_defined import FeatureConstructor
from kts.core.frame import KTSFrame
from kts.core.lists import feature_list
from kts.settings import cfg
from kts.stl.backend import Stacker
from kts.ui.docstring import HTMLReprWithDocstring
from kts.ui.feature_computing_report import FeatureComputingReport, SilentFeatureComputingReport
from kts.util.hashing import hash_list, hash_fold, hash_frame

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


class FeatureSet(HTMLReprWithDocstring):
    """Collects and computes feature constructors

    Args:
        before_split: list of regular features
        after_split: list of stateful features which may leak target if computed before split.
            They are run in Single Validation mode, i.e. for each fold they are fit using training objects
            and then applied to validation objects in inference mode.
        train_frame: a dataframe to perform training on. Should contain unique indices for each object.
        targets: list of target columns in case of a multilabel task, or a single string otherwise.
            Target columns may be computed. In this case the corresponding feature constructors
            should be passed to before_split list.
        auxiliary: list of auxiliary columns, such as datetime, groups or whatever else can be used
            for setting up your validation. These columns can be utilized by overriding Validator.
            As well as targets, auxiliary columns may be computed.
        description: any notes about this feature set.

    Examples:
        >>> fs = FeatureSet([feature_1, feature_2], [single_validation_feature],
        ...                  train_frame=train, targets='Survived')

        >>> fs = FeatureSet([feature_1, feature_2], [single_validation_feature],
        ...                  train_frame=train,
        ...                  targets=['Target1', 'Target2'], auxiliary=['date', 'metric_group'])

        >>> fs = FeatureSet([stl.select(['Age', 'Fare'])], [stl.mean_encode(['Embarked', 'Parch'], 'Survived')],
        ...                  train_frame=train, targets='Survived')
    """
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
        self.train_id = hash_frame(train_frame)[:8]
        self.test_frame = test_frame
        self.targets = self.check_columns(targets)
        self.auxiliary = self.check_columns(auxiliary)
        self.description = description

    def check_features(self, before, after):
        before = copy(before)
        after = copy(after)
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
        for i, feature in enumerate(after):
            if isinstance(feature, Stacker):
                before.append(after.pop(i))
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
        stackings = [i for i in self.before_split if isinstance(i, Stacker)]
        run_manager.run(stackings, frame, train=train, fold=self.train_id, ret=False, report=report)
        parallel = [i for i in self.before_split if i.parallel]
        run_manager.run(parallel, frame, train=train, fold=self.train_id, ret=False, report=report)
        run_manager.supervise(report)
        not_parallel = [i for i in self.before_split if not i.parallel]
        run_manager.run(not_parallel, frame, train=train, fold=self.train_id, ret=False, report=report)
        report.finish()
        run_manager.merge_scheduled()

    @property
    def target(self) -> Union[PreviewDataFrame, AnyFrame]:
        frame = self.train_frame
        self.compute()
        target_fcs = [fc for fc in self.before_split if len(set(self.targets) & set(fc.columns))]
        results = list(run_manager.run(target_fcs, frame, train=True, fold=self.train_id, ret=True).values())
        results.append(frame)
        for i, frame in enumerate(results):
            results[i] = frame[frame.columns.intersection(self.targets)]
        res = concat(results)
        if cfg.preview_mode:
            return PreviewDataFrame(res)
        return res

    @property
    def aux(self) -> Union[PreviewDataFrame, AnyFrame]:
        frame = self.train_frame
        self.compute()
        aux_fcs = [fc for fc in self.before_split if len(set(self.auxiliary) & set(fc.columns))]
        results = list(run_manager.run(aux_fcs, frame, train=True, fold=self.train_id, ret=True).values())
        results.append(frame)
        for i, frame in enumerate(results):
            results[i] = frame[frame.columns.intersection(self.auxiliary)]
        res = concat(results)
        if cfg.preview_mode:
            return PreviewDataFrame(res)
        return res

    def __getitem__(self, key) -> PreviewDataFrame:
        report = FeatureComputingReport()
        frame = self.train_frame[key]
        results = run_manager.run(self.features, frame, train=True, fold='preview', ret=True, report=report)
        result_frame = concat(results.values())
        run_manager.merge_scheduled()
        return PreviewDataFrame(result_frame)

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
        if self.requirements:
            elements += [ui.Annotation('requirements'), ui.Field('<tt>' + ', '.join(self.requirements) + '</tt>')]
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
        other_hashes = [hash_list(self.targets), hash_list(self.auxiliary), self.train_id]
        return f"FS{hash_list(sources_before, 2)}{hash_list(sources_after, 2)}{hash_list(other_hashes, 2)}"

    @property
    def requirements(self) -> Set[str]:
        result = set()
        for feature in self.features:
            if feature.requirements:
                result |= feature.requirements
        return result


class CVFeatureSet:
    def __init__(self, feature_set: FeatureSet, folds: List[Tuple[Collection, Collection]]):
        self.feature_set = copy(feature_set)
        self.fold_ids = []
        for idx_train, idx_valid in folds:
            assert len(set(idx_train) & set(idx_valid)) == 0 or all(idx_train == idx_valid), \
            "Train and valid sets should either not intersect or be equal"
            self.fold_ids.append(hash_fold(idx_train, idx_valid)[:8] + self.train_id)
        self.folds = folds
        self.n_folds = len(folds)
        self.columns_by_fold = defaultdict(lambda: None)

    def compute(self, frame: AnyFrame = None, report=None):
        if report is None:
            report = SilentFeatureComputingReport(feature_list)
        self.feature_set.compute(frame=frame, report=report)
        for i in range(self.n_folds):
            self.fold(i).compute(frame=frame, report=report, train=True)
            self.fold(i).compute(frame=frame, report=report, train=False)
        report.finish()
        run_manager.merge_scheduled()

    @property
    def computed(self) -> bool:
        raise NotImplementedError

    @property
    def train_frame(self):
        return self.feature_set.train_frame

    @property
    def train_id(self):
        return self.feature_set.train_id
    
    def fold(self, idx) -> 'Fold':
        return Fold(self, idx)

    @property
    def n_features(self) -> int:
        return max(map(len, self.columns_by_fold.values()))


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
        results.update(run_manager.run(self.before_split, frame, train=True, fold=self.train_id, ret=True))
        frame = self.train_frame
        results.update(run_manager.run(self.after_split, frame, train=True, fold=self.fold_id, ret=True))
        results.update({'_': self.train_frame[[]]})
        result_frame = concat(results.values())
        if self.columns is None:
            self.columns = list(result_frame.columns)
        else:
            assert all(self.columns == result_frame.columns)
        return result_frame.values

    @property
    def valid(self) -> np.ndarray:
        results = dict()
        frame = self.feature_set.train_frame
        results.update(run_manager.run(self.before_split, frame, train=True, fold=self.train_id, ret=True))
        frame = self.valid_frame
        results.update(run_manager.run(self.after_split, frame, train=False, fold=self.fold_id, ret=True))
        results.update({'_': self.valid_frame[[]]})
        result_frame = concat(results.values())
        if self.columns is None:
            raise UserWarning(".train should be called before .valid")
        assert all(self.columns == result_frame.columns)
        return result_frame.values

    def __call__(self, frame: AnyFrame) -> np.ndarray:
        results = dict()
        results.update(run_manager.run(self.before_split, frame, train=False, fold=self.train_id, ret=True))
        results.update(run_manager.run(self.after_split, frame, train=False, fold=self.fold_id, ret=True))
        result_frame = concat(results.values())
        if self.columns is None:
            raise UserWarning(".train should be called before inference")
        assert all(self.columns == result_frame.columns)
        return result_frame.values

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
    def train_id(self):
        return self.cv_feature_set.train_id

    @property
    def targets(self):
        return self.feature_set.targets

    @property
    def columns(self):
        return self.cv_feature_set.columns_by_fold[self.fold_idx]

    @columns.setter
    def columns(self, value):
        self.cv_feature_set.columns_by_fold[self.fold_idx] = value


def concat(frames: List[pd.DataFrame]):
    frames = list(frames)
    common_index = frames[0].index
    for frame in frames:
        common_index = common_index.intersection(frame.index)
    for i, frame in enumerate(frames):
        if len(frame.index) != len(common_index) or not all(frame.index == common_index):
            frames[i] = frame.loc[common_index]
    return pd.concat(frames, axis=1)
