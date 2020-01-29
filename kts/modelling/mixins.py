from kts.util.hashing import hash_dict, hash_str


class NamingMixin:
    @property
    def name(self):
        try:
            tracked_params = self.tracked_params
        except:
            tracked_params = self.get_tracked_params()
        params = {
            key: self.get_params()[key]
            for key in tracked_params if key in self.get_params()
        }

        name = self.__class__.__name__
        name += hash_dict(params, 3)
        if 'class_source' in dir(self):
            name += hash_str(self.class_source, 2)
        return name


class SourceMixin:
    @property
    def source(self):
        args = []
        for key, value in self.get_params().items():
            args.append(f"{key}={repr(value)}")
        res = ", ".join(args)
        return f"{self.__class__.__name__}({res})"


class PreprocessingMixin:
    def preprocess(self, X, y=None):
        return X, y

    def preprocess_fit(self, X, y, *args, **kwargs):
        X_proc, y_proc = self.preprocess(X, y)
        self.fit(X_proc, y_proc, *args, **kwargs)

    def preprocess_predict(self, X, *args, **kwargs):
        X_proc, _ = self.preprocess(X, None)
        return self.predict(X_proc, *args, **kwargs)


class ProgressMixin:
    def progress_callback(self, line):
        return {'success': False}

    def enable_verbosity(self):
        pass

    def get_n_steps(self):
        return 1


class Model(NamingMixin, SourceMixin, PreprocessingMixin, ProgressMixin):
    pass


class BinaryClassifierMixin(Model):
    def predict(self, X, **kwargs):
        return self.predict_proba(X, **kwargs)[:, 1]


class MultiClassifierMixin(Model):
    def predict(self, X, **kwargs):
        return self.predict_proba(X, **kwargs)


class RegressorMixin(Model):
    pass
