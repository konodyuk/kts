import sys
from kts.cli.utils import get_mode

STORAGE_PATH = "../storage/"
INDEX_COLUMN_PREFIX = "__kts__index_"
IS_PREVIEW_CALL = 0
MEMORY_LIMIT = 4 * (1024 ** 3)  # 4 Gb
GOAL = "MAXIMIZE"
MAX_LEN_SOURCE = 100

# CACHE_MODE = 'disk_and_ram'  # "disk", "disk_and_ram", "ram"
# CACHE_POLICY = 'everything'  # "everything", "service"

CACHE_MODE, CACHE_POLICY, ROOT_DIR = get_mode()

LB_DF_NAME = "__leaderboard"

SERVICE_DF_NAMES = [LB_DF_NAME]


if CACHE_MODE in ["disk", "disk_and_ram"]:
    sys.path.insert(0, ROOT_DIR)
    try:
        import kts_config as user_config
        from kts_config import *
    except:
        pass

# So dumb. I know.
# TODO: implement all this ... with pathlib
if not STORAGE_PATH.endswith("/"):
    STORAGE_PATH += "/"
