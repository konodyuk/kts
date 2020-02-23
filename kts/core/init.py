import inspect
import sys
from pathlib import Path

import ray

import kts.ui.settings
from kts.core import ui
from kts.core.backend.address_manager import get_address_manager, create_address_manager
from kts.core.cache import frame_cache, obj_cache
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

address_manager = None


def init():
    global address_manager
    cfg.scope = find_scope()
    cfg.stdout = sys.stdout
    config_path = find_config()
    if config_path is not None:
        cfg.load(config_path)
    kts.ui.settings.init()
    ray.init(ignore_reinit_error=True, logging_level=20 if cfg.debug else 50)
    try:
        address_manager = get_address_manager()
    except:
        address_manager = create_address_manager()
    if config_path is not None:
        frame_cache.path = cfg.storage_path
        obj_cache.path = cfg.storage_path
    if not cfg.debug:
        logger.level = 50
