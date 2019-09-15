import threading
import time

from .config import SCOPE
from .feature.storage import feature_list as features

def _define_in_scope():
	while True:
		features._define_in_scope(SCOPE)
		time.sleep(1)


def init(scope):
	SCOPE = scope

	scoping_thread = threading.Thread(target=_define_in_scope)
	scoping_thread.start()
