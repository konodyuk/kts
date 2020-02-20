from kts.core.backend.util import in_cli, in_worker, in_pytest


if not in_cli() and not in_worker() and not in_pytest():
    from kts.api import *
    from kts.core.init import init
    init()
