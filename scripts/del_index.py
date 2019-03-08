import os
import sys
import re
import random
sys.path.append(os.getcwd())

from time import sleep
from core.process import (read_config, write_config, read_mapping,
                     format_data, write_mapping, format_mapping, )
from utils.logger import Logger
from utils.mongo import Mongo
from elasticsearch import Elasticsearch


def main():
    config = read_config()
    mongo = config['mongo']
    elastic = config['elastic']
    # inital logging
    logger = Logger('del')

    esclient = Elasticsearch([elastic])

    for table in mongo['tables']:
        esclient.indices.delete(index=table, ignore=[400, 404])
        logger.record('Delete index: {}'.format(table))


if __name__ == '__main__':
    main()