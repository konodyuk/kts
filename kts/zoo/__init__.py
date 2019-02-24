from . import classification
from . import binary_classification
from . import regression
from ..model import Model
import forge

model_classes = []
for model_class in module.__dict__.items():
    if isinstance(model_class, Model) and model_class.__name__ is not 'Model':
        for module in (classification,
                       binary_classification,
                       regression):
            model_classes.append(model_class)

for model_class in model_classes:
    model_class.__init__ = forge.copy(model_class.Estimator.__init__)(Model.__init__)
