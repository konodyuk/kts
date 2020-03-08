import time

import kts.core.backend.signal as rs


class TextChunk(rs.Signal):
    def __init__(self, timestamp, text, run_id=None):
        self.timestamp = timestamp
        self.text = text
        self.run_id = run_id

    def get_contents(self):
        res = {'timestamp': self.timestamp, 'text': self.text}
        if self.run_id is not None:
            res['run_id'] = self.run_id
        return res


class RemoteTextIO:
    def __init__(self, run_id=None):
        self.buf = ""
        self.run_id = run_id

    def write(self, b):
        self.buf += b
        if self.buf.find('\n') != -1:
            self.flush()

    def flush(self):
        if self.buf:
            for line in self.buf.split('\n'):
                if line:
                    rs.send(TextChunk(time.time(), line + '\n', self.run_id))
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
