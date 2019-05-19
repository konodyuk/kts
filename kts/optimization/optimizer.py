from skopt import Optimizer as SKOptimizer
from skopt.utils import create_result
from skopt.plots import plot_convergence, plot_evaluations, plot_objective
from IPython.display import clear_output
import matplotlib.pyplot as plt

plt.style.use('dark_background') # such a bad thing, it should rather be specified in kts_config.py file in a kts-project

class Optimizer:
    def __init__(self, model, featureset, target, validator, goal='maximize', search_spaces=None):
        raise NotImplementedError
        if isinstance(search_spaces, type(None)):
            self.search_spaces = type(model).search_spaces
        else:
            self.search_spaces = search_spaces
        self.parameter_names = list(self.search_spaces.keys())
        self.value_spaces = list(self.search_spaces.values())
        self.opt = SKOptimizer(self.value_spaces)
        self.model = model
        self.featureset = featureset
        self.target = target
        self.validator = validator
        self.goal = goal
        if self.goal == 'maximize':
            self.coeff = -1
        elif self.goal == 'minimize':
            self.coeff = 1
        else:
            raise ValueError('Goal should be either to maximize or minimize objective.')
    
    def optimize(self, n_iters):
        for i in range(n_iters):
            pt = self.opt.ask()
            self.model.params = {k: v for k, v in zip(self.parameter_names, 
                                                      pt)}
            val = self.coeff * self.validator.score(self.model, self.featureset)
            clear_output(True)
            plot_convergence(self.opt.tell(pt, val));
            plt.show()

    def plot_objective(self):
        res = create_result(Xi=self.opt.Xi, 
                            yi=self.opt.yi, 
                            space=self.opt.space, 
                            rng=self.opt.rng, 
                            models=self.opt.models)
        plot_objective(res, dimensions=self.parameter_names);
        plt.show()
        
        
    def plot_evaluations(self):
        res = create_result(Xi=self.opt.Xi, 
                            yi=self.opt.yi, 
                            space=self.opt.space, 
                            rng=self.opt.rng, 
                            models=self.opt.models)
        plot_objective(res, dimensions=self.parameter_names);
        plt.show()