import time


class Stats:
    def __init__(self, df):
        self.data = dict()
        self.data['input_shape'] = df.shape

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.data['took'] = time.time() - self.start
