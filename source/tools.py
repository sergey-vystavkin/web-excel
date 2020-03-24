import time
import os
import json
from collections import OrderedDict
from shutil import rmtree
from configparser import ConfigParser, ExtendedInterpolation
import logging.config


def read_config(logger):
    try:
        config = ConfigParser(interpolation=ExtendedInterpolation(), delimiters=(':',))
        config.read_file(open(os.path.join('settings', 'config.cfg')))
        logger.info(config.get('info', 'config_read'))
        return config
    except Exception:
        logger.error('Config is not declared.')
        raise Exception


def get_output_map(config, logger):
    if not os.path.exists(config.get('path', 'mapping')):
        logger.error(config.get('error', 'output_columns_path').format(config.get('path', 'mapping')))
        raise Exception
    try:
        with open(config.get('path', 'mapping'), 'r') as reader:
            input_json = reader.read()
        json_map = json.loads(input_json)
        return OrderedDict(json_map)
    except Exception:
        logger.error(config.get('error', 'output_columns').format(config.get('path', 'mapping')))
        raise Exception


def retry(attempts=3, sleep_time=5):
    def inner_function(function):
        def wrapper(*args, **kwargs):
            for i in range(attempts):
                try:
                    return function(*args, **kwargs)
                except Exception as e:
                    if i + 1 == attempts:
                        raise e
                    time.sleep(sleep_time)
        return wrapper
    return inner_function


def delete_temp(config):
    if os.path.exists(config.get('path', 'temp')):
        rmtree(config.get('path', 'temp'), ignore_errors=True)


def make_loger():
    logging_config = os.path.join('settings', 'logging.cfg')
    default_conf_path = os.path.join('log', 'FPDS_bot.log')
    try:
        logging.config.fileConfig(logging_config)
    except Exception:
        if not os.path.exists(os.path.dirname(default_conf_path)):
            os.mkdir(os.path.dirname(default_conf_path))
        logging.basicConfig(filename=default_conf_path, level=logging.INFO,
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logging.warning(f'Something wrong with {logging_config} file, bot create default log file {default_conf_path}.')
    return logging
