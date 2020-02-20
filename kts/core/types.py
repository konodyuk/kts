from typing import Union

import pandas as pd

from kts.core.frame import KTSFrame

AnyFrame = Union[pd.DataFrame, KTSFrame]
