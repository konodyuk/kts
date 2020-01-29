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

    def load(self, path):
        self.config_path = path
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
        if self.config_path is not None:
            self._storage_path = self.config_path.parent / value
        else:
            self._storage_path = value

    @property
    def theme(self):
        return self._theme

    @theme.setter
    def theme(self, value):
        from kts.core.ui import set_theme
        set_theme(value)
        self._theme = value

    @property
    def highlighting(self):
        return self._highlighting

    @highlighting.setter
    def highlighting(self, value):
        from kts.core.ui import set_highlighting
        set_highlighting(value)
        self._highlighting = value

cfg = Config()
