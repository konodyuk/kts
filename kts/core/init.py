import inspect
import sys
from pathlib import Path

import ray

import kts.ui.settings
from kts.core.backend.address_manager import get_address_manager, create_address_manager
from kts.core.backend.signal import get_signal_manager, create_signal_manager
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

address_manager = None
signal_manager = None

def init():
    global address_manager, signal_manager
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
    ray.init(ignore_reinit_error=True, logging_level=20 if cfg.debug else 50)
    kts.ui.settings.init()
    try:
        address_manager = get_address_manager()
    except:
        address_manager = create_address_manager()
    try:
        signal_manager = get_signal_manager()
    except:
        signal_manager = create_signal_manager()
    if not cfg.debug:
        logger.level = 50
