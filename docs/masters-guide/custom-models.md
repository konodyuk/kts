# Custom Models

To use a model not contained in `kts.zoo`, you  need to define its class and specify tracked hyperparameters:

```python
from nonamelib import NoNameClassifier

class KTSNNClassifier(kts.CustomModel, NoNameClassifier):
    def get_tracked_params(self):
        return ['learning_rate', 'parameter_name', 'etc']
        
nnc = KTSNNClassifier(parameter_name=42)
val.score(nnc, fs_1)
```

You can also specify preprocessing \(later it will be available in a more convenient way\):

```python
from nonamelib import NoNameClassifier

class KTSNNClassifier(kts.CustomModel, NoNameClassifier):
    def get_tracked_params(self):
        return ['learning_rate', 'parameter_name', 'etc']
        
    def preprocess(self, X, y):
        new_X = <normalize_and_fillna>(X)
        return new_X, y
```

