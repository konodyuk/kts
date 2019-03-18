from . import classification
from . import binary_classification
from . import regression
from .. import modelling
# import forge
#
# exclude_types = [model_class for model_class in modelling.__dict__.values()
#                  if isinstance(model_class, type) and modelling.Model in model_class.__bases__] + [modelling.Model]
#
# model_classes = []
# for module in (classification, binary_classification, regression):
#     model_classes += [model_class for model_class in module.__dict__.values()
#                       if isinstance(model_class, type) and modelling.Model in model_class.__bases__]
#
# model_classes = set(model_classes) - set(exclude_types)
#
#
# for model_class in model_classes:
#     model_class.__init__ = forge.copy(model_class.Estimator.__init__)(modelling.Model.__init__)
#     model_class.__doc__ = model_class.Estimator.__doc__

