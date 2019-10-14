from kts import config
from kts.core import dataframe
from kts.core.backend import memory
from kts.util import source_utils, cache_utils
from kts.util.misc import captcha


class FeatureConstructor:
    """ """
    def __init__(self, function, cache_default=True, stl=False):
        self.function = function
        self.cache_default = cache_default
        self.__name__ = function.__name__
        self.source = source_utils.get_source(function)
        self.stl = stl
        self.args = dict()

    # needs refactoring because of direct storing source
    def __call__(self, df, cache=None, **kwargs):
        if not self.stl:
            self = memory.cache.load_obj(self.__name__ + "_fc")
        ktdf = dataframe.DataFrame(df=df)
        if type(cache) == type(None):
            cache = self.cache_default
        if not cache or config.IS_PREVIEW_CALL:  # written to avoid caching when @preview function uses @registered function inside
            return self.function(ktdf, **kwargs)

        name = f"{self.function.__name__}__{cache_utils.get_hash_df(ktdf)[:4]}__{ktdf.slice_id[-4:]}"
        name_metadata = name + "_meta"
        if memory.cache.is_cached_df(name):
            if memory.cache.is_cached_obj(name_metadata):
                cached_encoders = memory.cache.load_obj(name_metadata)
                for key, value in cached_encoders.items():
                    ktdf.encoders[key] = value
            return dataframe.DataFrame(
                df=memory.cache.load_df(name),
                train=ktdf.train,
                encoders=ktdf.encoders,
                slice_id=ktdf.slice_id,
            )
        else:
            result = self.function(ktdf)
            try:
                memory.cache.cache_df(result, name)
            except MemoryError:
                print(f"The dataframe is too large to be cached. "
                      f"It is {cache_utils.get_df_volume(result)}, "
                      f"but current memory limit is {config.MEMORY_LIMIT}.")
                print(f"Setting memory limit to {cache_utils.get_df_volume(result) * 2}")
                ans = input('Please confirm memory limit change. Enter "No" to cancel it.')
                do_change = True
                if ans.lower() == "no":
                    if captcha():
                        do_change = False
                if do_change:
                    memory.cache.set_memory_limit(
                        cache_utils.get_df_volume(result) * 2)
                    memory.cache.cache_df(result, name)
                    print("Done. Please don't forget to update your kts_config.py file.")
                else:
                    print("Cancelled")
            if ktdf.encoders:
                memory.cache.cache_obj(ktdf.encoders, name_metadata)
            return dataframe.DataFrame(
                df=result,
                train=ktdf.train,
                encoders=ktdf.encoders,
                slice_id=ktdf.slice_id,
            )

    def __repr__(self):
        if self.stl:
            return self.source
        else:
            return f'<Feature Constructor "{self.__name__}">'

    def __str__(self):
        return self.__name__

    def __sub__(self, other):
        assert isinstance(other, list)
        import kts.core.base_constructors

        return kts.core.base_constructors.compose([self, kts.core.base_constructors.column_dropper(other)])

    def __add__(self, other):
        assert isinstance(other, list)
        import kts.core.base_constructors

        return kts.core.base_constructors.compose([self, kts.core.base_constructors.column_selector(other)])

    def __mul__(self, other):
        assert isinstance(other, FeatureConstructor)
        import kts.core.base_constructors

        return kts.core.base_constructors.compose([self, other])