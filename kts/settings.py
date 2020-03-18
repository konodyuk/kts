import sys
from pathlib import Path

import psutil
import toml


class Config:
    def __init__(self):
        self.config_path = None
        self._storage_path = Path('../storage')
        self._threads = psutil.cpu_count(logical=False)
        self._theme = None
        self._highlighting = None
        self._theme_displays = []
        self.preview_mode = True
        self.debug = False
        self.stdout = sys.stdout
        self.feature_computing_report = None
        self.inference_report = None

    def load(self, path):
        self.config_path = Path(path)
        for k, v in toml.load(path).items():
            if k in dir(self):
                cls = self.__dict__[k].__class__
                self.__setattr__(k, cls(v))

    @property
    def threads(self):
        return self._threads

    @threads.setter
    def threads(self, value):
        self._threads = value

    @property
    def storage_path(self):
        return self._storage_path

    @storage_path.setter
    def storage_path(self, value):
        from kts.core.cache import obj_cache, frame_cache
        if value is None:
            self._storage_path = None
            obj_cache.path = None
            frame_cache.path = None
            return
        if self.config_path is not None:
            self._storage_path = Path(self.config_path.parent) / value
        else:
            self._storage_path = Path(value)
        obj_cache.path = self._storage_path
        frame_cache.path = self._storage_path

    @property
    def theme(self):
        return self._theme

    @theme.setter
    def theme(self, value):
        from kts.ui.settings import set_theme
        set_theme(value)
        self._theme = value

    @property
    def highlighting(self):
        return self._highlighting

    @highlighting.setter
    def highlighting(self, value):
        from kts.ui.settings import set_highlighting
        set_highlighting(value)
        self._highlighting = value

cfg = Config()
