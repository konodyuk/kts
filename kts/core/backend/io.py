import time

from ray.experimental import signal as rs


class TextChunk(rs.Signal):
    def __init__(self, timestamp, text):
        self.timestamp = timestamp
        self.text = text

    def get_contents(self):
        return {'timestamp': self.timestamp, 'text': self.text}


class RemoteTextIO:
    def __init__(self):
        self.buf = ""

    def write(self, b):
        self.buf += b
        if b.find('\n') != -1:
            self.flush()

    def flush(self):
        if self.buf:
            rs.send(TextChunk(time.time(), self.buf))
        self.buf = ""


class LocalTextIO:
    def __init__(self, report, run_id):
        self.buf = ""
        self.report = report
        self.run_id = run_id

    def write(self, b):
        self.buf += b
        if b.find('\n') != -1:
            self.flush()

    def flush(self):
        if self.buf:
            self.report.update_text(self.run_id, timestamp=time.time(), text=self.buf)
        self.buf = ""


class SuppressIO:
    def __init__(self):
        pass

    def write(self, b):
        pass

    def flush(self):
        pass
