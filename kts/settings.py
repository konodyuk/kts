import sys
from pathlib import Path

import psutil
import toml


class ConfigError(BaseException):
    pass


class Config:
    def __init__(self):
        self.config_path = None
        self._storage_path = Path('../storage')
        self._cpu_count = psutil.cpu_count(logical=True)
        self._memory = psutil.virtual_memory().total
        self._theme = None
        self._theme_name = 'light-orange'
        self._highlighter = None
        self._highlighter_name = 'tango'
        self._theme_handle = None
        self._dashboard_handles = list()
        self.preview_mode = True
        self.debug = False
        self.stdout = sys.stdout
        self.feature_computing_report = None
        self.inference_report = None

    def load(self, path):
        self.config_path = Path(path)
        for k, v in toml.load(path).items():
            if k in dir(self):
                try:
                    cls = getattr(self, k).__class__
                    self.__setattr__(k, cls(v))
                except BaseException as e:
                    if isinstance(e, ConfigError):
                        raise e
                    raise ConfigError(f"Invalid {k}: {v}")

    @property
    def cpu_count(self):
        return self._cpu_count

    @cpu_count.setter
    def cpu_count(self, value):
        self._cpu_count = value

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
        new_path = Path(value)
        if self.config_path is not None:
            rel_path = Path(self.config_path.parent) / value
            if rel_path.exists():
                new_path = rel_path
        if new_path.exists():
            self._storage_path = new_path
        else:
            raise ConfigError(f"{new_path} does not exist")
        obj_cache.path = self._storage_path
        frame_cache.path = self._storage_path

    @property
    def theme(self) -> str:
        return self._theme_name

    @theme.setter
    def theme(self, value):
        if self._theme is None:
            self._theme_name = value
            return
        from kts.ui.settings import set_theme
        set_theme(value)

    @property
    def highlighting(self) -> str:
        return self._highlighter_name

    @highlighting.setter
    def highlighting(self, value):
        if self._highlighter is None:
            self._highlighter_name = value
            return
        from kts.ui.settings import set_highlighting
        set_highlighting(value)

cfg = Config()
