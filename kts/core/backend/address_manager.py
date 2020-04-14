import os
import time

import ray

try:
    from ray.experimental import get_actor  # ray<=0.8.1
except ImportError:
    from ray.util import get_actor          # ray>=0.8.2


@ray.remote
class AddressManager:
    def __init__(self):
        self.data = dict()
        self.is_none = dict()
        self.timestamps = dict()

    def get(self, key):
        return self.data[key]

    def put(self, entry):
        key, value, is_none = entry
        self.data[key] = value
        self.is_none[key] = is_none
        self.timestamps[key] = time.time()

    def has(self, key):
        return key in self.data

    def isnone(self, key):
        return self.is_none[key]

    def timestamp(self, key):
        return self.timestamps[key]

    def confirm(self, key):
        self.timestamps[key] = time.time()

    def ls(self):
        return list(self.data.keys())

    def clear(self):
        self.data.clear()
        self.timestamps.clear()

    def delete(self, key):
        self.is_none[key] = True


pid = os.getpid()

def get_address_manager():
    return get_actor(f"AddressManager{pid}")

def create_address_manager():
    return AddressManager.options(name=f"AddressManager{pid}").remote()
