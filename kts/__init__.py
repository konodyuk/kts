from kts.__version__ import __version__
from kts.core.backend.util import in_cli, in_worker, in_pytest

if not in_cli() and not in_worker() and not in_pytest():
    from kts.core.init import init
    init()

    import kts.stl as stl
    from kts.api.decorators import preview, feature, helper, generic, delete
    from kts.core.backend.progress import pbar
    from kts.core.cache import save, load, rm, ls
    from kts.core.feature_set import FeatureSet
    from kts.core.frame import KTSFrame
    from kts.core.lists import feature_list as features, helper_list as helpers
    from kts.feature_selection import *
    from kts.modelling.custom_model import CustomModel, custom_model
    from kts.modelling.mixins import NormalizeFillNAMixin
    from kts.ui.dashboard import dashboard
    from kts.ui.settings import set_highlighting, set_theme, set_animation
    from kts.validation.leaderboard import leaderboard, leaderboard_list
    from kts.validation.validator import Validator
    from kts.validation.split import Refiner

    lb = leaderboard
    lbs = leaderboard_list
