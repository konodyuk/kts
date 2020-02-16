import time
from typing import Iterable, Optional

from ray.experimental import signal as rs


class AbstractProgressBar:
    def update_time(self, step, timestamp):
        if self.start is None:
            self.start = timestamp
            return
        self.took = timestamp - self.start
        if step > 0:
            self.eta = (self.total - self.step) / step * self.took


class LocalProgressBar(AbstractProgressBar):
    _min_interval = 0.2
    report = None

    def __init__(self, iterable: Iterable, total: Optional[int] = None):
    # def __init__(self, iterable, total=None):
        self.iterable = iterable
        self.total = total
        if total is None:
            try:
                self.total = len(self.iterable)
            except:
                pass
        self.took = None
        self.eta = None

    def __iter__(self):
        last_update = 0.
        for i, o in enumerate(self.iterable):
            yield o
            cur = time.time()
            self.update_time(i, cur)
            if cur - last_update >= self._min_interval:
                # rs.send(ProgressSignal(value=i + 1, total=self.total))
                self.report.update(self.run_id, self.value, self.total, self.took, self.eta)
                last_update = cur
        # rs.send(ProgressSignal(value=self.total, total=self.total))
        self.report.update(self.run_id, self.total, self.total, self.took, self.eta)


class ProgressSignal(rs.Signal):
    def __init__(self, value, total, took, eta):
        self.value = value
        self.total = total
        self.took = took
        self.eta = eta

    def get_percentage(self):
        return self.value / self.total * 100

    def get_contents(self):
        return {'value': self.value, 'total': self.total, 'took': self.took, 'eta': self.eta}


class RemoteProgressBar(AbstractProgressBar):
    _min_interval = 0.2

    def __init__(self, iterable: Iterable, total: Optional[int] = None):
    # def __init__(self, iterable, total=None):
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

    def __iter__(self):
        last_update = 0.
        for i, o in enumerate(self.iterable):
            yield o
            cur = time.time()
            self.update_time(i, cur)
            if cur - last_update >= self._min_interval:
                rs.send(ProgressSignal(value=i + 1, total=self.total, took=self.took, eta=self.eta))
                last_update = cur
        rs.send(ProgressSignal(value=self.total, total=self.total, took=self.took, eta=None))


pbar = RemoteProgressBar
