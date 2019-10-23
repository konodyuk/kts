from collections import MutableSequence

from kts.core.backend import memory


class FeatureList(MutableSequence):
    """ """
    def __init__(self):
        self.full_name = "kts.api.feature.feature_list"  # such a hardcode
        self.names = [self.full_name]
        while self.names[-1].count("."):
            self.names.append(self.names[-1][self.names[-1].find(".") + 1:])
        self.names.append("kts.features")
        while self.names[-1].count("."):
            self.names.append(self.names[-1][self.names[-1].find(".") + 1:])
        self.functors = []
        self.name_to_idx = dict()

    def recalc(self):
        """ """
        self.functors = []
        self.name_to_idx = dict()
        names = [
            obj for obj in memory.cache.cached_objs() if obj.endswith("_fc")
        ]
        for idx, name in enumerate(names):
            functor = memory.cache.load_obj(name)
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
            raise TypeError("Index should be int, slice or str")

    def __delitem__(self, key):
        raise AttributeError("This object is read-only")

    def __setitem__(self, key, value):
        raise AttributeError("This object is read-only")

    def insert(self, key, value):
        """

        Args:
          key: 
          value: 

        Returns:

        """
        raise AttributeError("This object is read-only")

    def define_in_scope(self, global_scope):
        """

        Args:
          global_scope: 

        Returns:

        """
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
        return len(self.functors)


feature_list = FeatureList()
