import time
from collections import defaultdict
from contextlib import redirect_stdout
from datetime import datetime

from IPython.core.display import display

from kts.settings import cfg
from kts.ui.components import HTMLRepr, Field, Annotation, InnerColumn, Row, AlignedColumns, Output, Progress
from kts.util.formatting import format_value


class SingleFoldReport(HTMLRepr):
    def __init__(self, value, total, took, eta, output=None):
        self.value = value
        self.total = total
        self.took = took
        self.eta = eta
        self.output = output

    @property
    def html(self):
        tmp = [Progress(self.value, self.total, **self.progress_formatting)]
        if self.output is not None:
            tmp += [Output(self.output)]
        return Row([
            InnerColumn(tmp),
            Field(format_value(self.took, time=True), **self.indicator_formatting),
            Field(format_value(self.eta, time=True), **self.indicator_formatting)]).html

    @property
    def indicator_formatting(self):
        return dict(bg=False, style=f"padding: 0px 5px; margin: 2px; width: 7em;")

    @property
    def progress_formatting(self):
        return dict(style="margin-top: 3px; width: 450px;")


class FeatureComputingReport(HTMLRepr):
    def __init__(self, feature_constructors=None):
        self.entries = defaultdict(lambda: dict(value=0, total=0, took=None, eta=None))
        self.outputs = defaultdict(lambda: list())
        self.feature_constructors = feature_constructors
        self.handle = None
        self.update_interval = 0.5
        self.last_update = -1
        self.changed = False

    @property
    def html(self):
        # thumbnails = [i.html_collapsable(**self.thumbnail_formatting) for i in self.active_features]
        thumbnails = [Field(i, **self.thumbnail_formatting).html for i in self.active_feature_names]
        blocks = self.progress_blocks
        return AlignedColumns([
            [Annotation('feature', **self.thumbnail_annotation_formatting).html] + thumbnails,
            [Annotation('progress', **self.progress_annotation_formatting).html] + blocks
        ], title='computing features').html

    @property
    def active_features(self):
        return [self.feature_constructors[name] for name in self.active_feature_names]

    @property
    def active_feature_names(self):
        result = list()
        for key in self.entries.keys():
            name = key.function_name
            if name not in result:
                result.append(name)
        return result

    @property
    def progress_blocks(self):
        blocks = []
        for name in self.active_feature_names:
            entries = self.entries_by_name(name)
            single_fold_reports = []
            for k, v in entries.items():
                output = self.outputs.get(k, None)
                if output is not None:
                    output = self.format_output(output)
                sfr = SingleFoldReport(v['value'], v['total'], v['took'], v['eta'], output)
                single_fold_reports.append(sfr)
            blocks.append(InnerColumn(single_fold_reports).html)
        return blocks

    def format_output(self, output):
        return '\n'.join([f"[{self.format_timestamp(timestamp)}] {text}" for timestamp, text in output])

    def entries_by_name(self, name):
        return {key: value for key, value in self.entries.items() if key.function_name == name}

    @property
    def thumbnail_formatting(self):
        return dict(style='padding: 0px 5px; text-align: right; margin: 2px;', bg=False)

    @property
    def thumbnail_annotation_formatting(self):
        return dict(style="text-align: right; margin-bottom: 3px; margin-right: 5px;")

    @property
    def progress_annotation_formatting(self):
        return dict(style="margin-bottom: 3px; margin-left: 5px;")

    def format_timestamp(self, timestamp):
        return datetime.fromtimestamp(timestamp).strftime("%H:%M:%S.%f")[:-3]

    def update(self, run_id, value, total, took=None, eta=None, autorefresh=True):
        self.changed = True
        if self.entries[run_id]['value'] <= value or value == total:
            entry = self.entries[run_id]
            entry['value'] = value
            entry['total'] = total
            if took is not None:
                entry['took'] = took
            if eta is not None:
                entry['eta'] = eta
        if autorefresh:
            self.refresh()

    def update_text(self, run_id, text=None, timestamp=None, autorefresh=True):
        self.changed = True
        output = self.outputs[run_id]
        if (timestamp, text) not in output:
            output.append((timestamp, text))
        if autorefresh:
            self.refresh()

    def completed(self):
        self.refresh()
        for entry in self.entries.values():
            if entry['value'] < entry['total']:
                return False
        return True

    def finish(self):
        for entry in self.entries.values():
            entry['value'] = entry['total']
        self.refresh(force=True)

    def show(self):
        return display(self, display_id=True)

    def refresh(self, force=False):
        if not self.entries:
            return
        if not self.changed and not force:
            return
        self.changed = False
        if self.handle is None:
            with redirect_stdout(cfg.stdout):
                self.handle = self.show()
        if self.handle is None:
            # not in ipython
            return
        if force or time.time() - self.last_update >= self.update_interval:
            with redirect_stdout(cfg.stdout):
                self.handle.update(self)
            self.last_update = time.time()


class SilentFeatureComputingReport:
    def __init__(self, *args, **kwargs):
        pass

    def update(self, *args, **kwargs):
        pass

    def update_text(self, *args, **kwargs):
        pass

    def finish(self):
        pass

    def refresh(self, *args, **kwargs):
        pass
