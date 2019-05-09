from ..storage import caching


# class Helper:
#     def __init__(self, function):
#         self.function = function
#         self.__name__ = function.__name__
#         self.source = source_utils.get_source(function)
#         self.__call__ = self.function.__call__
#
#     # # @functools.wraps(source_utils.get_source)
#     def __call__(self, *args, **kwargs):
#         return self.function(*args, **kwargs)
#
#     # def __getattribute__(self, item):
#     #     print(item)
#     #     return getattr(self.function, item)
#
#     def __repr__(self):
#         return self.function.__name__
#
#     def __str__(self):
#         return self.__name__
#
#

from collections import MutableSequence


class HelperList(MutableSequence):
    def __init__(self):
        self.full_name = "kts.feature.helper.helper_list"  # such a hardcode
        self.names = [self.full_name]
        while self.names[-1].count('.'):
            self.names.append(self.names[-1][self.names[-1].find('.') + 1:])
        self.names.append('kts.helpers')
        while self.names[-1].count('.'):
            self.names.append(self.names[-1][self.names[-1].find('.') + 1:])
        self.objects = []
        self.name_to_idx = dict()

    def recalc(self):
        self.objects = []
        self.name_to_idx = dict()
        names = [obj for obj in caching.cache.cached_objs() if obj.endswith('_helper')]
        for idx, name in enumerate(names):
            functor = caching.cache.load_obj(name)
            self.objects.append(functor)
            self.name_to_idx[functor.__name__] = idx

    def __repr__(self):
        self.recalc()
        string = f"[{', '.join([f.__name__ for f in self.objects])}]"
        return string

    def __getitem__(self, key):
        self.recalc()
        if type(key) in [int, slice]:
            return self.objects[key]
        elif type(key) == str:
            return self.objects[self.name_to_idx[key]]
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
                except BaseException:
                    pass

    def __len__(self):
        self.recalc()
        return len(self.objects)


helper_list = HelperList()