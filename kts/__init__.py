import sys

if not sys.argv[0].endswith('kts') and not sys.argv[0].endswith('default_worker.py'):
    from kts.api import *
    from kts.core.init import init
    init()
