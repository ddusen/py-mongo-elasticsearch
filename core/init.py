import os
import sys
import re
import random
sys.path.append(os.getcwd())

from time import sleep
from process import (read_config, write_config, read_mapping,
                     format_data, write_mapping, format_mapping, )
from utils.logger import Logger
from utils.mongo import Mongo

# Time to wait for data or connection.
_SLEEP = 1


class Init:

    def __init__(self):
        # inital config
        config = read_config()
        self.mongo = config['mongo']

        # inital logging
        self.logger = Logger('init')

    def _build_mapping(self):
        '''
        根据config文件中的mongo配置，把mongo的表结构转换成es的mapping
        '''
        self.logger.record('Starting：build mapping...')

        for table in self.mongo['tables']:
            
            # build new es mapping
            self.logger.record('build new es mapping:{}'.format(table))
            old_mapping = read_mapping(table)

            # build
            self.mongo['table'] = table
            client = Mongo(self.mongo)
            total = client.count()
            offset = 0
            # 随机向前移动 n 步：1⃣减少重复操作 2⃣均匀生成mapping
            increase = lambda : random.randint(1, 100)
            while offset <= total:
                self.logger.record('mapping: {}.json, total number:{}, current number:{}'.format(table, total, offset))

                for query in client.find_one(offset=offset):

                    del query['_id']

                    format_mapping(old_mapping['mappings'][table]['properties'], query)

                    write_mapping(table, old_mapping)

                offset += increase()

            sleep(_SLEEP)

        self.logger.record('Ending：build mapping.')


if __name__ == '__main__':
    init = Init()
    init._build_mapping()
