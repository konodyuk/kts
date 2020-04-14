import atexit
import os
from glob import glob

import ray

from kts.core.backend.address_manager import create_address_manager
from kts.core.backend.signal import create_signal_manager
from kts.settings import cfg


address_manager = None
signal_manager = None
heartbeat_handle = None
ray_session_filename = f'/tmp/kts.ray_session.{os.getpid()}'


def write_ray_session(address):
    with open(ray_session_filename, 'w') as f:
        f.write(address)


def read_ray_session():
    ray_sessions = glob(f'/tmp/kts.ray_session.*')
    if not ray_sessions:
        return
    ray_sessions = sorted(ray_sessions, key=os.path.getmtime)
    address = open(ray_sessions[-1]).read().strip()
    return address


def remove_ray_session():
    if os.path.exists(ray_session_filename):
        os.remove(ray_session_filename)


def setup_ray():
    global address_manager, signal_manager, heartbeat_handle
    address = read_ray_session()
    ray_cluster_info = ray.init(address=address, ignore_reinit_error=True, logging_level=20 if cfg.debug else 50)
    if not address and ray_cluster_info:
        address = ray_cluster_info['redis_address']
        write_ray_session(address)
    address_manager = create_address_manager()
    signal_manager = create_signal_manager()
    heartbeat_handle = ray.put(1)


def ensure_ray():
    try:
        ray.get(heartbeat_handle, timeout=0)
    except:
        stop_ray()
        setup_ray()


def stop_ray():
    remove_ray_session()
    ray.shutdown()


atexit.register(remove_ray_session)
