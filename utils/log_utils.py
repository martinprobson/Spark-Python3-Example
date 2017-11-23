'''
Created on 22 Nov 2017

@author: martinr
'''

import os
import logging.config
import json

def setupLogging(default_path='logging.json',default_level=logging.INFO,env_key='LOG_CFG'):
    ''' Setup logging from configuration

    Parameters
    ----------
    default_path :  optional, default = 'logging.json'
        Path to (json format) logging configuration file. Defaults to 
        'logging.json' in current directory if not set.
    default_level :  optional, default = logging.INFO
        default logging level - set to INFO if not provided.
    env_key: optional, default = 'LOG_CFG'
        Name of env variable that points to path of config file.
    '''
    
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)

#TODO: Setup proper unittest
if __name__ == '__main__':
    setupLogging()
    logger = logging.getLogger(__name__)
    logger.info("test....")