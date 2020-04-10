import time
from contextlib import redirect_stdout

import numpy as np
from IPython.display import display

from kts.settings import cfg
from kts.ui.components import HTMLRepr, Column, Field, Annotation, Title, InnerColumn, Row, Progress, Raw
from kts.ui.plotting import Line, Plot
from kts.util.formatting import format_value


class SingleModelFittingReport:
    def __init__(self, n_steps=None, metric_name=None):
        self.n_steps = n_steps
        self.step = 0
        self.train_scores = []
        self.valid_scores = []
        self.current_train_score = None
        self.current_valid_score = None
        self.metric_name = metric_name
        self.metric_value = None
        self.start = None
        self.took = None
        self.eta = None

    def update(self, step, train_score=None, valid_score=None):
        self.step = max(step, self.step)
        if self.start is None:
            self.start = time.time()
        else:
            self.took = time.time() - self.start
            self.eta = (self.n_steps - self.step) / max(self.step, 1) * self.took
        if train_score is not None:
            self.train_scores.append((step, train_score))
            self.train_scores = sorted(self.train_scores, key=lambda x: x[0])
            self.current_train_score = self.train_scores[-1][1]
        if valid_score is not None:
            self.valid_scores.append((step, valid_score))
            self.valid_scores = sorted(self.valid_scores, key=lambda x: x[0])
            self.current_valid_score = self.valid_scores[-1][1]

    def finish(self):
        self.step = self.n_steps

    def set_metric_value(self, value):
        self.metric_value = value

    def clip_outliers(self, lines):
        res_lower = np.inf
        res_upper = -np.inf
        for a in lines:
            if len(a) < 20:
                continue
            x = list(zip(*a))[0]
            y = list(zip(*a))[1]
            spl = np.array_split(y, 20)
            maximums = list(map(np.max, spl))
            minimums = list(map(np.min, spl))
            if np.mean(spl[0]) < np.mean(spl[1]):
                lower = min(np.min(spl[-1]), np.mean(y))
                upper = np.max(y)
            else:
                lower = np.min(y)
                upper = max(np.max(spl[-1]), np.mean(y))
            res_lower = min(lower, res_lower)
            res_upper = max(upper, res_upper)
        if res_lower == np.inf:
            return lines
        res = []
        for a in lines:
            x = list(zip(*a))[0]
            y = list(zip(*a))[1]
            new_x = x
            new_y = np.clip(y, res_lower, res_upper)
            if len(new_y) > 100:
                spl_x = np.array_split(new_x, 100)
                spl_y = np.array_split(new_y, 100)
                new_x = list(map(np.mean, spl_x))
                new_y = list(map(np.mean, spl_y))
            res.append(list(zip(new_x, new_y)))
        return res

    def get_first(self, a):
        return [i[0] for i in a]

    def get_second(self, a):
        return [i[1] for i in a]

    def html(self, active=False, annotations=False):
        ind_kw = dict(style='width: 4.5em; padding-top: 0px; padding-bottom: 0px;', bg=False, bold=True)
        indicators = Row([
            InnerColumn([Annotation('progress')] * (annotations) + [Progress(self.step, self.n_steps, style='width: 400px; margin-top: 5px;')]), # calc(600px - 12rem - 150px)
            InnerColumn([Annotation('train')] * (annotations) + [Field(format_value(self.current_train_score), **ind_kw, accent=False)]),
            InnerColumn([Annotation('valid')] * (annotations) + [Field(format_value(self.current_valid_score), **ind_kw)]),
            InnerColumn([Annotation(self.metric_name)] * (annotations) + [Field(format_value(self.metric_value), **ind_kw)]),
            InnerColumn([Annotation('took')] * (annotations) + [Field(format_value(self.took, True), **ind_kw)]),
            InnerColumn([Annotation('eta')] * (annotations) + [Field(format_value(self.eta, True), **ind_kw)]),
        ])
        if active:
            lines = []
            colors = []
            if len(self.train_scores) >= 5:
                lines.append(self.train_scores)
                colors.append(cfg._theme.third)
            if len(self.valid_scores) >= 5:
                lines.append(self.valid_scores)
                colors.append(cfg._theme.accent)
            if len(lines) >= 1:
                lines = self.clip_outliers(lines)
                for i in range(len(lines)):
                    steps = list(zip(*lines[i]))[0]
                    scores = list(zip(*lines[i]))[1]
                    lines[i] = Line(steps, scores, colors[i])
                indicators.elements[0].elements.append(Field(Plot(lines, height=200, width=385).html, bg=False, style='padding-right: 0px; padding-left: 5px;'))
                return indicators.html
            else:
                return indicators.html
        else:
            return indicators.html


class CVFittingReport(HTMLRepr):
    def __init__(self, n_folds=5, n_steps=None, metric_name='metric'):
        self.n_folds = n_folds
        self.reports = [SingleModelFittingReport(n_steps, metric_name) for i in range(n_folds)]
        self.active = set()
        self.current_fold = None

    def set_fold(self, value):
        self.current_fold = value
        self.active.clear()
        self.active.add(value)

    def update(self, step, train_score=None, valid_score=None):
        self.reports[self.current_fold].update(step, train_score, valid_score)

    def set_metric(self, value):
        self.reports[self.current_fold].set_metric_value(value)
        self.reports[self.current_fold].finish()

    @property
    def html(self):
        return Column([Title('fitting')] + [Raw(report.html(active=(i in self.active), annotations=(i == 0))) for i, report in enumerate(self.reports)]).html


class InferenceReportBlock(HTMLRepr):
    def __init__(self, id, n_folds=5):
        self.id = id
        self.n_folds = n_folds
        self.fold = 0
        self.start = None
        self.took = None
        self.eta = None

    def update(self, fold):
        self.fold = max(fold, self.fold)
        if self.start is None:
            self.start = time.time()
        else:
            self.took = time.time() - self.start
            self.eta = (self.n_folds - self.fold) / (self.fold) * self.took

    def finish(self):
        self.update(self.n_folds)
        self.eta = None

    def html(self, annotations):
        ind_kw = dict(style='width: 4.5em; padding-top: 0px; padding-bottom: 0px;', bg=False, bold=True)
        return Row([
            InnerColumn([Annotation('id')] * (annotations) + [Field(self.id, **ind_kw)]),
            InnerColumn([Annotation('progress')] * (annotations) + [Progress(self.fold, self.n_folds, style='width: 400px; margin-top: 5px;')]),
            InnerColumn([Annotation('took')] * (annotations) + [Field(format_value(self.took, True), **ind_kw)]),
            InnerColumn([Annotation('eta')] * (annotations) + [Field(format_value(self.eta, True), **ind_kw)]),
        ])


class InferenceReport(HTMLRepr):
    def __init__(self):
        self.current_report = -1
        self.reports = []
        self.handle = None

    def start(self, id, n_folds):
        self.current_report = len(self.reports)
        self.reports.append(InferenceReportBlock(id, n_folds))
        self.refresh()

    def update(self, fold):
        self.reports[self.current_report].update(fold)
        self.refresh()

    def finish(self):
        self.reports[self.current_report].finish()
        self.refresh()

    @property
    def html(self):
        return Column([Title('inference')] + [report.html(i==0) for i, report in enumerate(self.reports)]).html

    def show(self):
        return display(self, display_id=True)

    def refresh(self):
        if self.handle is None:
            with redirect_stdout(cfg.stdout):
                self.handle = self.show()
        if self.handle is None:
            # not in ipython
            return
        with redirect_stdout(cfg.stdout):
            self.handle.update(self)
