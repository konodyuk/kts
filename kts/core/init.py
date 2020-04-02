import inspect
import sys
from pathlib import Path

import kts.ui.settings
from kts.core.backend.ray_middleware import setup_ray
from kts.core.cache import frame_cache, obj_cache
from kts.core.lists import feature_list, helper_list
from kts.settings import cfg
from kts.util.debug import logger


def find_scope():
    frame = inspect.currentframe()
    while frame is not None and 'get_ipython' not in frame.f_globals:
        frame = frame.f_back
    if frame is not None:
        return frame.f_globals
    else:
        return None

def find_config():
    p = Path('.').cwd()
    while p != p.parent and not (p / 'kts_config.toml').exists():
        p = p.parent
    config_path = (p / 'kts_config.toml')
    if config_path.exists():
        return config_path
    else:
        return None


def init():
    cfg.scope = find_scope()
    cfg.stdout = sys.stdout
    config_path = find_config()
    if config_path is not None:
        cfg.load(config_path)
    if config_path is not None:
        frame_cache.path = cfg.storage_path
        obj_cache.path = cfg.storage_path
    feature_list.sync()
    helper_list.sync()
    kts.ui.settings.init()
    setup_ray()
    if not cfg.debug:
        logger.level = 50
