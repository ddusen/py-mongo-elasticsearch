import re
import os
import sys
sys.path.append(os.getcwd())

from time import sleep
from pymongo import MongoClient, ASCENDING
from pymongo.cursor import CursorType
from pymongo.errors import AutoReconnect
from bson.timestamp import Timestamp

from process import (read_config, write_config, read_mapping,
                     format_data, )
from utils.logger import Logger
from utils.mongo import Mongo
from utils.elastic import Elastic

# Time to wait for data or connection.
_SLEEP = 1


class Sync:

    def __init__(self):
        # inital config
        config = read_config()
        self.mongo = config['mongo']
        self.elastic = config['elastic']
        self.oplog = config['oplog']

        # inital logging
        self.logger = Logger('sync')
        self.es = Elastic(self.elastic)

    # 基于 SQL 语句的全量同步
    def _full_sql(self):
        self.logger.record('Starting：based full sql...')

        for table in self.mongo['tables']:

            # create new es index
            self.logger.record('create new es index:{}'.format(table))
            self.es.init(table, mapping=read_mapping(table))

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
                actions = []
                action_ids = []
                for q in queryset:
                    # 数据库ID -> 文档ID
                    doc_id = str(q['_id'])
                    del q['_id']
                    # format data
                    doc = q
                    format_data(doc)

                    #业务相关操作: 时段聚合
                    if 'sales_date' in doc and doc['sales_date']:
                        m_d_h = re.compile(r'....-(..)-(..) (..):.*?').findall(doc['sales_date'])[0]
                        doc['sales_date_dict'] = {'month': m_d_h[0], 'day': m_d_h[1], 'hour': m_d_h[2]}
                    else:
                        doc['sales_date_dict'] = {'month': '', 'day': '', 'hour': ''}

                    action_ids.append(doc_id)
                    actions.append({
                        "_index": table,
                        "_type":  table,
                        "_id": doc_id,
                        "_source": doc
                    })

                # elastic save for batch
                self.es.insert_batch(table, actions, action_ids)

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

                        #业务相关操作: 时段聚合
                        if 'terminal_open_time' in doc and doc['terminal_open_time']:
                            m_d_h = re.compile(r'....-(..)-(..) (..):.*?').findall(doc['terminal_open_time'])[0]
                            doc['terminal_open_time_dict'] = {'month': m_d_h[0], 'day': m_d_h[1], 'hour': m_d_h[2]}
                        else:
                            doc['terminal_open_time_dict'] = {'month': '', 'day': '', 'hour': ''}

                        if op is 'u':
                            self.es.update(table, doc_id, doc)
                        elif op is 'i':
                            self.es.insert(table, doc_id, doc)
                        elif op is 'd':
                            self.es.delete(table, doc_id)

                        # 记录增量位置
                        write_config('oplog', 'ts', stamp)
                    sleep(_SLEEP)

            except AutoReconnect:
                sleep(_SLEEP)

        self.logger.record('Ending：based increase oplog.')


if __name__ == '__main__':
    sync = Sync()
    sync._full_sql()
    sync._inc_oplog()
