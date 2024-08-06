import logging

import numpy as np

logger = logging.getLogger(__name__)


class Objective:

    def __init__(self, benchmark, fitness_name, history=None):
        # Gets the fitness funciton as defined in fitness.py
        self.benchmark = benchmark
        self.history = history
        self.fitness_fn, metrics_needed = get_fitness_fn(fitness_name)
        self.setup_metrics(metrics_needed)        
    
    def setup_metrics(self, metrics=['dbsize', 'qphh', 'time']):
        # Metrics we have at hand to compute fitness
        
        metric_fn = {
            'dbsize': self.benchmark.get_storage_size,
            'qphh': self.benchmark.get_qphh,
            'time': self.benchmark.get_runtime,
        }

        self.metrics = {}

        # Set up metrics we need to use to compute the fitness
        for metric in metrics: 
            self.metrics[metric] = metric_fn[metric]

    def get_state_metrics(self, individual):
        # Set up the database indexes using the provided individual
        self.benchmark.db.apply_vector(individual)
        metric_dict = {}
        for metric, metric_fn in self.metrics.items():            
            logger.debug(f'Computing metric {metric} via {metric_fn}')
            __result__ = metric_fn()
            logger.debug(f'Computed {metric}. Results {__result__}')
            metric_dict.update(__result__)        

        # Get all the benchmark metrics
        return metric_dict
    
    def eval_baseline(self, baseline_individual):
        logger.info('Evaluating baseline individual')
        metrics = self.get_state_metrics(baseline_individual)
        self.baseline_metrics = metrics
        self.history.update(baseline_individual, self.baseline_metrics)
        logger.debug('Baseline metrics {}'.format(self.baseline_metrics))

    def evaluate(self, individual,):
        logger.info('Evaluating individual {}'.format(individual))        
        
        # Apply state to the DB and get metrics from benchmark
        metrics = self.get_state_metrics(individual)
        logger.debug(f'Metrics: {metrics}')

        # Debug stuff to make sure the current state 
        # is the same as the provided individual
        state = self.benchmark.db.get_current_state_vector()
        logger.debug(f'Current db state: {state}')
        logger.info(f'Evaluation result: {metrics}')

        # Calculate the fitness function using the provided metrics
        fitness = self.fitness_fn(metrics, self.baseline_metrics)
        logger.info(f'Fitness result: {fitness:5.4f}')
        
        # Logging stuff
        metrics['fitness'] = fitness
        self.history.update(individual, metrics)
        self.history.serialize()
        
        return (fitness,)
    
    def fake_eval(self, individual):        
        return (float(10.5),)


def fake_metric(individual):
    metrics = {
        'power': 1293 * np.abs(np.abs(np.random.randn())),
        'throughput': 2300 * np.abs(np.abs(np.random.randn())),
        'qphh': 2560 * np.abs(np.random.randn()),
        'db_size': 1250 * np.abs(np.random.randn()),
        'time': 82.4 * np.abs(np.random.randn()),
        'cost': -1,
        'evaluation_time': 1212 * np.abs(np.random.randn())
    }
    return metrics


def default_fitness(current_metrics, baseline_metrics):
    qphh = current_metrics['qphh']
    return float(qphh)


def qphh_prop_fitness(current_metrics, baseline_metrics):
    qphh = current_metrics['qphh']
    baseline_qphh = baseline_metrics['qphh']
    fitness = baseline_qphh/float(qphh)
    return float(fitness)


def time_fitness(current_metrics, baseline_metrics):
    current_time = current_metrics['time']
    baseline_time = baseline_metrics['time']
    return baseline_time/float(current_time)


def time_squared_fitness(current_metrics, baseline_metrics):
    current_time = current_metrics['time']
    baseline_time = baseline_metrics['time']
    return np.square(baseline_time/float(current_time))

# Never use this 
# this is just for debug purposes 
def dbsize_fitness(current_metrics, baseline_metrics):
    current = current_metrics['index_size']    
    return current


def get_fitness_fn(fitness_opt):
    function = __fitness__[fitness_opt]['function']
    metrics = __fitness__[fitness_opt]['needed_metrics']
    return function, metrics


def get_available_fitness():
    return __fitness__.keys()


__fitness__ = {
    'qphh': {
        'function': default_fitness,
        'needed_metrics': [
            'qphh', 
            'dbsize',
            'time'
        ]
    },
    'time': {
        'function': time_fitness,
        'needed_metrics': [
            'time',
            'dbsize',
        ]
    },
    'time_squared': {
        'function': time_squared_fitness,
        'needed_metrics': [
            'time',
            'dbsize'
        ]
    },
    'dbsize': {
        'function': dbsize_fitness,
        'needed_metrics': [
            'time',
            'dbsize',            
        ]
    },    
}
