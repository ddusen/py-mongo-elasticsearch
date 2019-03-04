import os
import sys
import re
sys.path.append(os.getcwd())

from time import sleep
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch.exceptions import (NotFoundError,
                                      ConflictError, RequestError, )
from pymongo import MongoClient, ASCENDING
from pymongo.cursor import CursorType
from pymongo.errors import AutoReconnect
from bson.timestamp import Timestamp

from process import (read_config, write_config, read_mapping,
                     format_data, )
from utils.logger import Logger
from utils.mongo import Mongo

# Time to wait for data or connection.
_SLEEP = 3


class Sync:

    def __init__(self):
        # inital config
        config = read_config()
        self.mongo = config['mongo']
        self.elastic = config['elastic']
        self.oplog = config['oplog']

        # inital logging
        self.logger = Logger('sync')

    # 基于 SQL 语句的全量同步
    def _full_sql(self):
        self.logger.record('Starting：based full sql...')

        for table in self.mongo['tables']:

            # create new es doc_type
            self.logger.record('create new es doc_type:{}'.format(table))
            self._elastic(self.mongo['db'], table, doc=read_mapping(table), option='init')

            # sync
            self.mongo['table'] = table
            client = Mongo(self.mongo)
            total = client.count()
            offset = 0
            limit = 100
            while offset <= total:

                # record offset and limit
                self.logger.record('offset:{}, limit:{}'.format(offset, limit))

                queryset = client.find(offset=offset, limit=limit)
                for q in queryset:
                    # 数据库ID -> 文档ID
                    doc_id = str(q['_id'])
                    del q['_id']
                    # format data
                    doc = q
                    format_data(doc)
                    # elastic save
                    self._elastic(self.mongo['db'], table, doc_id, doc, option='create')

                sleep(_SLEEP)
                offset += limit

        self.logger.record('Ending：based full sql.')

    # 基于 oplog 日志的增量同步
    def _inc_oplog(self):
        self.logger.record('Starting：based increase oplog...')

        # sync
        oplog = Mongo(self.mongo).client().local.oplog.rs
        # 获取偏移量
        stamp = oplog.find().sort('$natural',ASCENDING).limit(-1).next()['ts'] if not self.oplog['ts'] else self.oplog['ts']

        while True:
            kw = {}

            kw['filter'] = {'ts': {'$gt': eval(stamp)}}
            kw['cursor_type'] = CursorType.TAILABLE_AWAIT
            kw['oplog_replay'] = True

            cursor = oplog.find(**kw)
            try:

                while cursor.alive:
                    for q in cursor:
                        stamp = q['ts']

                        # Do something with doc.
                        op = q['op'] # 操作 u i d
                        db, table = q['ns'].split('.') # 表的变动 saas_dq_uat.pos
                        doc = q['o']
                        
                        # 数据库ID -> 文档ID
                        doc_id = str(doc['_id'])
                        del doc['_id']

                        # format data
                        format_data(doc)

                        if op is 'u':
                            self._elastic(db, table, doc_id, doc, option='update')
                        elif op is 'i':
                            self._elastic(db, table, doc_id, doc, option='create')
                        elif op is 'd':
                            self._elastic(db, table, doc_id, option='delete')

                        # 记录增量位置
                        write_config('oplog', 'ts', stamp)
                    sleep(_SLEEP)

            except AutoReconnect:
                sleep(_SLEEP)

        self.logger.record('Ending：based increase oplog.')

    # elastic
    def _elastic(self, index=None, doc_type=None, doc_id=None, doc={}, option='create'):
        """
        option: 
            init: 初始化文档结构。（当config.ini中的init为True时才会执行。）
            create: 若文档已存在，则不执行任何操作。 若文档不存在，则直接创建。
            update: 若文档已存在，则直接更新。 若文档不存在，则不执行任何操作。
            delete: 若文档已存在，则直接删除。若文档不存在，则不执行任何操作。
        """
        esclient = Elasticsearch([self.elastic])

        status = 'Success !'

        index = self.elastic['index'] if not index else index
        doc_type = self.elastic['type'] if not doc_type else doc_type

        if 'create' == option:
            try:
                esclient.create(
                    index=index,
                    doc_type=doc_type,
                    id=doc_id,
                    body=doc,
                )
            except ConflictError:
                status = 'Fail(existsd) !'

        elif 'update' == option:
            try:
                esclient.update(
                    index=index,
                    doc_type=doc_type,
                    id=doc_id,
                    body={'doc': doc},
                )
            except NotFoundError:
                status = 'Fail(not existsd) !'

        elif 'delete' == option:
            try:
                esclient.delete(
                    index=index,
                    doc_type=doc_type,
                    id=doc_id,
                )
            except NotFoundError:
                status = 'Fail(not existsd) !'

        elif 'init' == option:
            try:
                IndicesClient(esclient).create(
                    index=index,
                    body=doc,
                )
            except RequestError:
                status = 'Fail(existsd) !'

        self.logger.record('Sync@%s < %s-%s-%s > %s' % (option,
                                                        index,
                                                        doc_type,
                                                        doc_id,
                                                        status,
                                                        ))


if __name__ == '__main__':
    sync = Sync()
    sync._full_sql()
    sync._inc_oplog()
