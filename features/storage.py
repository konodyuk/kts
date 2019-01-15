from .. import config
from . import utils
import glob

class FeatureConstructor:
    def __init__(self, function, no_cache_default=False):
        self.function = function
        self.no_cache_default = no_cache_default
        self.__name__ = function.__name__
        self.src = utils.get_src(function)
        
    # needs refactoring because of direct storing source
    def __call__(self, df, no_cache=False):
        if no_cache:
            return self.function(df)
        if utils.is_cached(self.function, df):
            return utils.load_cached(self.function, df)
        else:
            result = self.function(df)
            utils.cache(self.function, df, result)
            return result
        
    @property
    def source(self):
        return self.src
        return utils.load_src_func(self.function)()
    
    def __repr__(self):
        return f'<Feature Constructor "{self.__name__}">'
        
    def __str__(self):
        return self.__name__

# actually it doesn't wprk
class FeatureList(list):
    def __init__(self):
        self.full_name = "kts.storage.feature_constructors" # such a hardcode 
        self.names = [self.full_name]
        while self.names[-1].count('.'):
            self.names.append(self.names[-1][self.names[-1].find('.') + 1:])
        self.functors = []
        self.name_to_idx = dict()
    
    def recalc(self):
        self.functors = []
        self.name_to_idx = dict()
        for idx, file in enumerate(glob.glob(config.feature_path + '*.fc')):
            functor = utils.load_fc(file)
            self.functors.append(functor)
            self.name_to_idx[functor.__name__] = idx
    
    def __repr__(self):
        self.recalc()
        string = f"[{', '.join([f.__str__() for f in self.functors])}]"
        return string
        
    def __getitem__(self, key):
        self.recalc()
        if type(key) == int:
            return self.functors[key]
        elif type(key) == str:
            return self.functors[self.name_to_idx[key]]
        else:
            raise TypeError('Index should be either int or str')
    
    def define_in_scope(self, global_scope):
        self.recalc()
        for func in self.name_to_idx:
            for name in self.names:
                try:
                    exec(f"{func} = {name}['{func}']", global_scope)
                    break
                except Exception as e:
                    pass
    
feature_constructors = FeatureList()