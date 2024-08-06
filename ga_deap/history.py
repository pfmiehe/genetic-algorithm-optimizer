import os
import logging
from collections import defaultdict

import utils

logger = logging.getLogger(__name__)

class History:

    def __init__(self, path, file_name='history.json', tensorboard=False):
        utils.ensure_dir(path)        
        self.path = path        
        self.filepath = os.path.join(self.path, file_name)
        self.generation = 0
        self.history = defaultdict(dict)
        self.tensorboard = tensorboard
        self.n_individuals = 0
        '''
            history = {
                generation_x: {
                    individual: {
                        all_metrics_here
                    }
                }
            }
        '''
        logger.info(f'Recording history dump to {self.filepath}')

        if tensorboard:
            from tensorboardX import SummaryWriter
            self.tb_writer = SummaryWriter(self.path)
            logger.debug(f'Using tensorboard at {self.path}')

    def update(self, individual, metrics: dict):
        logger.debug('History - updating metrics')
        individual = ' '.join([str(x) for x in individual])
        self.n_individuals += 1
        if self.tensorboard:
            for metric, value in metrics.items():                                
                self.tb_writer.add_scalar(f'metrics/{metric.title()}', value, self.n_individuals)
                logger.debug(f'Writing to tensorboard {metric.title(), value}')
        self.history[self.generation][individual] = metrics
    
    def update_generation(self,):        
        self.generation += 1
        if self.tensorboard:
            self.tb_writer.add_scalar('train/Generations', self.generation, self.generation)
        logger.debug(f'Updated generation to {self.generation}')
    
    def serialize(self):
        logger.debug(f'Updating history logs on disk at {self.filepath}')
        utils.save_json(self.filepath, self.history)
