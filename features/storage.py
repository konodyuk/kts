from .. import config
from . import utils
from . import decorators
import glob

# @property
# def feature_constructors(storage):
#     functions = []
#     for file in glob.glob(config.feature_path + '*.py'):
#         src = utils.load_src(file)
#         exec(src)
#         functions.append(eval(file.split('/')[-1].split('.')[~1]))
#     return functions

# import mprop
# mprop.init()

# actually it doesn't wprk
class FeatureList(list):
    def __init__(self):
        self.functions = []
        self.name_to_idx = dict()
    
    def recalc(self):
        self.functions = []
        self.name_to_idx = dict()
        for idx, file in enumerate(glob.glob(config.feature_path + '*.py')):
            src = utils.load_src(file)
            exec(src)
            func_name = utils.get_func_name(src)
            self.functions.append(eval(func_name))
            self.name_to_idx[func_name] = idx
    
    def __repr__(self):
        self.recalc()
        string = f"[{', '.join([f.__name__ for f in self.functions])}]"
        return string
        
    def __getitem__(self, key):
        if type(key) == int:
            return self.functions[key]
        elif type(key) == str:
            return self.functions[self.name_to_idx[key]]
        else:
            raise TypeError('Index should be either int or str')
    
feature_constructors = FeatureList()