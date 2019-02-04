from .. import config
from . import utils
from . import stl
import glob
import os

class FeatureConstructor:
    def __init__(self, function, cache_default=True):
        self.function = function
        self.cache_default = cache_default
        self.__name__ = function.__name__
        self.source = utils.get_src(function)
        self.stl = False
        
    # needs refactoring because of direct storing source
    def __call__(self, df, cache=None):
        if type(cache) == type(None):
            cache = self.cache_default
        if not cache or config.test_call: # dirty hack to avoid  caching when @test function uses @registered function inside
            return self.function(df)
        if utils.is_cached(self.function, df):
            return utils.load_cached(self.function, df)
        else:
            result = self.function(df)
            utils.cache(self.function, df, result)
            return result
    
    def __repr__(self):
        return f'<Feature Constructor "{self.__name__}">'
        
    def __str__(self):
        return self.__name__
    
class FeatureSet:
    def __init__(self, fc_before, fc_after=None, df_input=None):
        self.fc_before = fc_before
        self.fc_after = fc_after
        if type(df_input) != type(None):
            self.set_df(df_input)
            
    def set_df(self, df_input):
        self.df_input = df_input
        self.df = self.fc_before(self.df_input)
        
    def __call__(self, df):
        return stl.merge([
            self.fc_before(self.df_input),
            self.fc_after(self.df_input)
        ])
        
    def __getitem__(self, idx):
        return stl.merge([
            self.df.iloc[idx], 
            self.fc_after(self.df_input.iloc[idx])
        ])
    
    @property
    def source(self):
        import inspect
        used_funcs = (self.features_before + self.features_after)[::-1]
        for func in used_funcs:
            for func_stored in feature_list:
                if func_stored.__name__ in func.source and \
                func_stored.__name__ not in [i.__name__ for i in used_funcs]:
                    used_funcs.append(func_stored)
        src = '\n'.join([i.source for i in used_funcs[::-1]]) 
        
        src += '\n\n'
#         src += inspect.getsource(type(self))
#         src += '\n\n'
        src += 'featureset = '
        src += type(self).__name__ + '('
        src += 'features_before=[' + ', '.join([i.__name__ for i in self.features_before]) + '], '
        src += 'features_after=[' + ', '.join([i.__name__ for i in self.features_after]) + ']'
        src += ')'
        return src
    
from collections import MutableSequence

class FeatureList(MutableSequence):
    def __init__(self):
        self.full_name = "kts.feature.storage.feature_list" # such a hardcode 
        self.names = [self.full_name]
        while self.names[-1].count('.'):
            self.names.append(self.names[-1][self.names[-1].find('.') + 1:])
        self.functors = []
        self.name_to_idx = dict()
    
    def recalc(self):
        self.functors = []
        self.name_to_idx = dict()
        files = glob.glob(config.feature_path + '*.fc')
        files.sort(key=os.path.getmtime)
        for idx, file in enumerate(files):
            functor = utils.load_fc(file)
            self.functors.append(functor)
            self.name_to_idx[functor.__name__] = idx
    
    def __repr__(self):
        self.recalc()
        string = f"[{', '.join([f.__str__() for f in self.functors])}]"
        return string
        
    def __getitem__(self, key):
        self.recalc()
        if type(key) in [int, slice]:
            return self.functors[key]
        elif type(key) == str:
            return self.functors[self.name_to_idx[key]]
        else:
            raise TypeError('Index should be int, slice or str')
            
    def __delitem__(self, key):
        raise AttributeError('This object is read-only')
        
    def __setitem__(self, key, value):
        raise AttributeError('This object is read-only')
        
    def insert(self, key, value):
        raise AttributeError('This object is read-only')
    
    def define_in_scope(self, global_scope):
        self.recalc()
        for func in self.name_to_idx:
            for name in self.names:
                try:
                    exec(f"{func} = {name}['{func}']", global_scope)
                    break
                except Exception as e:
                    pass
                
    def __len__(self):
        self.recalc()
        return len(self.functors)
    
feature_list = FeatureList()
