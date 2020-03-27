import time
from contextlib import contextmanager
from copy import copy
from typing import Iterable, Optional

import kts.core.backend.signal as rs
from kts.core.backend.util import in_worker


class AbstractProgressBar:
    def update_time(self, step, timestamp):
        if self.start is None:
            self.start = timestamp
            return
        self.took = timestamp - self.start
        if step > 0:
            self.eta = (self.total - step) / step * self.took


class LocalProgressBar(AbstractProgressBar):
    _min_interval = 0.2
    report = None
    run_id = None

    def __init__(self, iterable: Iterable, total: Optional[int] = None, title: Optional[str] = None):
        self.run_id = copy(self.__class__.run_id)
        self.iterable = iterable
        self.total = total
        if total is None:
            try:
                self.total = len(self.iterable)
            except:
                pass
        self.took = None
        self.eta = None
        self.start = None
        if title is not None:
            self.run_id.function_name += f" - {title}"

    def __iter__(self):
        last_update = 0.
        self.report.update(self.run_id, 0, self.total, None, None)
        self.update_time(0, time.time())
        for i, o in enumerate(self.iterable):
            yield o
            cur = time.time()
            self.update_time(i + 1, cur)
            if cur - last_update >= self._min_interval:
                self.report.update(self.run_id, i + 1, self.total, self.took, self.eta)
                last_update = cur
        self.report.update(self.run_id, self.total, self.total, self.took, self.eta)


class ProgressSignal(rs.Signal):
    def __init__(self, value, total, took, eta, title=None, run_id=None):
        self.value = value
        self.total = total
        self.took = took
        self.eta = eta
        self.title = title
        self.run_id = run_id

    def get_percentage(self):
        return self.value / self.total * 100

    def get_contents(self):
        res = {'value': self.value, 'total': self.total, 'took': self.took, 'eta': self.eta, 'title': self.title}
        if self.run_id is not None:
            res['run_id'] = self.run_id
        return res

    def __repr__(self):
        return f"ProgressSignal({self.value}/{self.total}, took={self.took}, eta={self.eta})"


class RemoteProgressBar(AbstractProgressBar):
    _min_interval = 0.2

    def __init__(self, iterable: Iterable, total: Optional[int] = None, title: Optional[str] = None):
        self.iterable = iterable
        self.total = total
        if total is None:
            try:
                self.total = len(self.iterable)
            except:
                pass
        self.took = None
        self.eta = None
        self.start = None
        self.title = title

    def __iter__(self):
        last_update = 0.
        rs.send(ProgressSignal(value=0, total=self.total, took=None, eta=None, title=self.title))
        self.update_time(0, time.time())
        for i, o in enumerate(self.iterable):
            yield o
            cur = time.time()
            self.update_time(i + 1, cur)
            if cur - last_update >= self._min_interval:
                rs.send(ProgressSignal(value=i + 1, total=self.total, took=self.took, eta=self.eta, title=self.title))
                last_update = cur
        rs.send(ProgressSignal(value=self.total, total=self.total, took=self.took, eta=None, title=self.title))


class KTSProgressBar:
    report = None
    run_id = None

    def __init__(self, iterable: Iterable, total: Optional[int] = None, title: Optional[str] = None):
        if in_worker():
            cls = RemoteProgressBar
        else:
            cls = LocalProgressBar
            cls.report = self.report
            cls.run_id = self.run_id
        self.internal = cls(iterable, total, title)

    def __iter__(self):
        yield from self.internal

    @classmethod
    @contextmanager
    def local_mode(cls, report, run_id):
        cls.report = report
        cls.run_id = run_id
        yield


pbar = KTSProgressBar
