import os
import sys
sys.path.append(os.getcwd())

from time import sleep
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch.client import IndicesClient
from elasticsearch.exceptions import (NotFoundError,
                                      ConflictError, RequestError, )
from utils.logger import Logger


class Elastic:

    def __init__(self, conf):
        self.esclient = Elasticsearch([{
            'host': conf.get('host'), 
            'port': conf.get('port')
        }])
        self.logger = Logger('elastic')

    def search(self, index, body=None):
        """
            search: 根据查询条件搜索文档，返回匹配结果
        """
        body = {"query": {"match_all": {}}} if not body else body
        return self.esclient.search(index=index, body=body)

    def init(self, index, mapping):
        """
            init: 初始化文档结构
        """
        try:
            IndicesClient(self.esclient).create(
                index=index,
                body=mapping,
            )
            self.logger.record('{}\nInit <{}> Success !'.format('-'*100, index))
        except RequestError:
            self.logger.record('{}\nInit <{}> Fail(existsd) !'.format('-'*100, index))

    def insert_batch(self, index, actions, action_ids):
        """
            insert_batch: 批量插入。若文档已存在，则不执行任何操作。 若文档不存在，则直接创建。
        """
        helpers.bulk(self.esclient, actions)
        self.logger.record('{}\nInsert Batch <{}> Success ! \n{}'.format('-'*100, index, action_ids))

    def insert(self, index, doc_id, doc):
        """
            insert: 若文档已存在，则不执行任何操作。 若文档不存在，则直接创建。
        """
        try:
            self.esclient.create(
                index=index,
                id=doc_id,
                doc_type=index,
                body=doc,
            )
            self.logger.record('{}\nInsert <{}> <{}> Success !'.format('-'*100, index, doc_id))
        except ConflictError:
            self.logger.record('{}\nInsert <{}> <{}> Fail(existsd) !'.format('-'*100, index, doc_id))

    def update(self, index, doc_id, doc):
        """
            update: 若文档已存在，则直接更新。 若文档不存在，则不执行任何操作。
        """
        try:
            self.esclient.update(
                index=index,
                id=doc_id,
                doc_type=index,
                body={'doc': doc},
            )
            self.logger.record('{}\nUpdate <{}> <{}> Success !'.format('-'*100, index, doc_id))
        except NotFoundError:
            self.logger.record('{}\nUpdate <{}> <{}> Fail(not existsd) !'.format('-'*100, index, doc_id))

    def delete(self, index, doc_id):
        """
            delete: 若文档已存在，则直接删除。若文档不存在，则不执行任何操作。
        """
        try:
            self.esclient.delete(
                index=index,
                id=doc_id,
                doc_type=index,
            )
            self.logger.record('{}\nDelete <{}> <{}> Success !'.format('-'*100, index, doc_id))
        except NotFoundError:
            self.logger.record('{}\nDelete <{}> <{}> Fail(not existsd) !'.format('-'*100, index, doc_id))


if __name__ == '__main__':
    pass
