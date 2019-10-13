from fastprogress import progress_bar

from kts.modelling.mixins import ArithmeticMixin, PreprocessingMixin


class Pipeline(ArithmeticMixin, PreprocessingMixin):
    """ """
    def __init__(self, model, featureslice):
        self.model = model
        self.featureslice = featureslice
        self.__name__ = (self.model.__name__ + "-" +
                         self.featureslice.featureset.__name__)

    def fit(self, **kwargs):
        """

        Args:
          **kwargs: 

        Returns:

        """
        if "masterbar" in kwargs:
            mb = kwargs.pop("masterbar")
        else:
            mb = None
        if mb:
            pb = progress_bar(range(2), parent=mb)
            mb.child.comment = f"computing the features..."
            pb.on_iter_begin()
            pb.update(0)
        X = self.featureslice().values
        y = self.featureslice.target.values
        if mb:
            mb.child.comment = f"training..."
            pb.on_iter_begin()
            pb.update(1)
        try:
            self.model.preprocess_fit(
                X,
                y,
                eval_set=[(
                    self.featureslice(self.featureslice.idx_test).values,
                    self.featureslice.target.values[
                        self.featureslice.idx_test],
                )],
                **kwargs,
            )
        except:
            if mb:
                mb.child.comment = (
                    "failed to train with eval_set, training without it...")
            self.model.preprocess_fit(X, y, **kwargs)
        if mb:
            pb.update(2)

    def predict(self, df, **kwargs):
        """

        Args:
          df: 
          **kwargs: 

        Returns:

        """
        if "masterbar" in kwargs:
            mb = kwargs.pop("masterbar")
        else:
            mb = None
        if mb:
            pb = progress_bar(range(2), parent=mb)
            mb.child.comment = f"computing the features..."
            pb.on_iter_begin()
            pb.update(0)
        X = self.featureslice(df).values
        if mb:
            mb.child.comment = f"predicting..."
            pb.on_iter_begin()
            pb.update(1)
        res = self.model.preprocess_predict(X, **kwargs)
        if mb:
            pb.update(2)
        return res
