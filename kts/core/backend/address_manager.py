import time

import ray
from ray.experimental import get_actor


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


def get_address_manager():
    return get_actor('AddressManager')


def create_address_manager():
    return AddressManager.options(name="AddressManager").remote()
