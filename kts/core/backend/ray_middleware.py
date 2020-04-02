import ray

from kts.core.backend.address_manager import get_address_manager, create_address_manager
from kts.core.backend.signal import get_signal_manager, create_signal_manager
from kts.settings import cfg


address_manager = None
signal_manager = None
heartbeat_handle = None


def setup_ray():
    global address_manager, signal_manager, heartbeat_handle
    ray.init(ignore_reinit_error=True, logging_level=20 if cfg.debug else 50)
    try:
        address_manager = get_address_manager()
    except:
        address_manager = create_address_manager()
    try:
        signal_manager = get_signal_manager()
    except:
        signal_manager = create_signal_manager()
    heartbeat_handle = ray.put(1)


def ensure_ray():
    try:
        ray.get(heartbeat_handle, timeout=0)
    except:
        stop_ray()
        setup_ray()


def stop_ray():
    ray.shutdown()
