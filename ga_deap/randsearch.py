import logging
import random

import numpy as np

import utils
from benchmark import Benchmark
from database import Database
from deap import algorithms, base, creator, tools
from fitness import (Objective, fake_metric, get_available_fitness,
                     get_fitness_fn)
from history import History


def get_params():
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument('-p', '--pop_size', type=int, default=50)    
    parser.add_argument('-g', '--generations', type=int, default=100)
    parser.add_argument('-o', '--outpath', type=str, default='runs/')
    parser.add_argument('-f', '--fitness', type=str,
        default='qphh', choices=get_available_fitness())
    parser.add_argument('--debug', action='store_true')
    # WARNING: use this only for debugging fast
    parser.add_argument('--use_fake_eval', action='store_true')
    parser.add_argument('--use_tensorboard', action='store_true')

    args = parser.parse_args()
    print('\n* * * Arguments * * * ')
    print(args)
    return args


def train(args):

    # Get the connection string as defined in config.py file 
    connection_string = utils.get_conn_str()

    # Creates a database object for handling db connections
    # Queries and index creation
    database = Database(
        connection_string=connection_string,
        reset_indexes=True
    )

    # Number of columns to optimize indexing 
    # (i.e., size of each individual)
    state_size = database.state_size

    # Benchmark object allows running power testes, get storage size
    # query time and other performance metrics on the database
    benchmark = Benchmark(database)    

    # Record all metrics and save them to disk
    history = History(
        path=args.outpath, 
        file_name='history.json', 
        tensorboard=args.use_tensorboard
    )

    # Objective allows the fitness evaluation
    # by using the benchmark metrics
    objective = Objective(
        benchmark=benchmark, 
        fitness_name=args.fitness,
        history=history
    )

    if args.use_fake_eval:
        print('''ATTENTION: You are using fake metric values!.
                 No updates to the database would be made.''')
        objective.get_state_metrics = fake_metric

    # Using the initial state as baseline individual
    objective.eval_baseline([0] * state_size)

    
    toolbox = base.Toolbox()

    # Set up the evaluation of the fitness function
    toolbox.register("evaluate", objective.evaluate)    

    # Train the Genetic Algorithm for some generations
    NGEN = args.generations
    for gen in range(NGEN):
        history.update_generation()
        # Apply mutation and crossover on the population 
        offspring = np.random.randint(0, 2, size=(args.pop_size, state_size))
        # Evaluate the fitness of each individual
        fits = toolbox.map(toolbox.evaluate, offspring)
        # Log and assign the calculated fitness for each individual
        for fit, ind in zip(fits, offspring):
            logger.info(f'Evaluated ind {ind}, result: {fit}')                
        
    logger.info(f'Done.')



if __name__ == '__main__':    

    # Getting the command line parameters
    args = get_params()
    
    # Set up the log function 
    logging.basicConfig(
        format='%(asctime)s - [%(levelname)-8s] - %(message)s',
        level=logging.DEBUG if args.debug else logging.INFO
    )

    logger = logging.getLogger(__name__)

    # Let's train stuff
    train(args)
