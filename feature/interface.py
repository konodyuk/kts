from .decorators import test, register, deregister
from .storage import FeatureConstructor

def column_selector(columns):
    def __col_selector(df):
        return df[cols]
    return FeatureConstructor(__col_selector, cache_default=False)

def column_dropper(columns):
    def __col_dropper(df):
        return df.drop(columns, axis=1)
    return FeatureConstructor(__col_dropper, cache_default=False)

