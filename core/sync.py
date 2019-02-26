import os, sys, re
sys.path.append(os.getcwd())

from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch.exceptions import (NotFoundError, ConflictError, RequestError, )

from process import (read_config, init_elastic, format_pos, )
from utils.logger import Logger
from utils.mongo import Mongo


class Sync:

    def __init__(self):
        # inital config
        config = read_config()
        self.mongo = config['mongo']
        self.elastic = config['elastic']

        # inital logging
        self.logger = Logger('pos')

        # inital elastic doc types
        eval(init_elastic(self.elastic['init']))


    #基于 SQL 语句的全量同步    
    def _full_sql(self):
        self.logger.record('Starting：based full sql...')
        
        mongo = Mongo(self.mongo)
        total = mongo.count()
        offset = 0
        limit = 10
        while offset <= total:
            queryset = mongo.find(offset=offset, limit=limit)
            for q in queryset:
                # 数据库ID -> 文档ID
                doc_id = str(q['_id'])
                del q['_id']
                # format data
                doc = format_pos(q)
                # elastic save
                self._elastic(doc_id, doc, option='create')

            offset += limit

        self.logger.record('Ending：based full sql.')


    # elastic 
    def _elastic(self, doc_id=None, doc={}, option='create'):
        """
        option: 
            init: 初始化文档结构。（当config.ini中的init为True时才会执行。）
            create: 若文档已存在，则不执行任何操作。 若文档不存在，则直接创建。
            update: 若文档已存在，则直接更新。 若文档不存在，则不执行任何操作。
            delete: 若文档已存在，则直接删除。若文档不存在，则不执行任何操作。
        """
        esclient = Elasticsearch([self.elastic])
        
        status = 'Success !'

        if 'create' == option:
            try:
                esclient.create(
                        index=self.elastic['index'],
                        doc_type=self.elastic['type'],
                        id=doc_id,
                        body=doc,
                    )
            except ConflictError:
                status = 'Fail(existsd) !'
                
        elif 'update' == option:
            try:
                esclient.update(
                        index=self.elastic['index'],
                        doc_type=self.elastic['type'],
                        id=doc_id,
                        body={'doc':doc},
                    )
            except NotFoundError:
                status = 'Fail(not existsd) !'

        elif 'delete' == option:
            try:
                esclient.delete(
                        index=self.elastic['index'],
                        doc_type=self.elastic['type'],
                        id=doc_id,
                    )
            except NotFoundError:
                status = 'Fail(not existsd) !'

        elif 'init' == option:
            try:
                IndicesClient(esclient).create(
                    index=self.elastic['index'],
                    body=doc,
                )
            except RequestError:
                status = 'Fail(existsd) !'

        print('Sync@%s < %s-%s-%s > %s' % (option, self.elastic['index'], self.elastic['type'], doc_id, status, ))
        self.logger.record('Sync@%s < %s-%s-%s > %s' % (option, self.elastic['index'], self.elastic['type'], doc_id, status, ))


if __name__ == '__main__':
    sync = Sync()
    sync._full_sql()
