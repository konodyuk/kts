import sys

import ray

from kts.core.backend.address_manager import get_address_manager
from kts.core.frame import KTSFrame


def in_cli():
    return sys.argv[0].endswith('kts')


def in_worker():
    return sys.argv[0].endswith('default_worker.py')


def in_pytest():
    return sys.argv[0].endswith('pytest')


def safe_put(kf: KTSFrame):
    address_manager = get_address_manager()
    h = kf.hash()
    if ray.get(address_manager.has.remote(h)):
        oid = ray.get(address_manager.get.remote(h))
    else:
        oid = ray.put(kf)
        address_manager.put.remote((h, oid, False))
    return oid
