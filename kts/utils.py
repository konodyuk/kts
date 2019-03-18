import hashlib


def list_hash(lst, length):
    return hashlib.sha256(repr(tuple(lst)).encode()).hexdigest()[:length]