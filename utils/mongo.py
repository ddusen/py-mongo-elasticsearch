from pymongo import MongoClient


class Mongo:

    def __init__(self, config):
        '''
        config like this:
        config = {
            'host': '106.14.94.38',
            'port': 27071,
            'db': 'saas_dq_uat',
            'table': 'pos'
        }
        '''
        self.mongo_client = MongoClient(
            config['host'],
            config['port'],
            # username=config.get('user'),
            # password=config.get('passwd'),
            # authSource=config.get('authSource'),
            # authMechanism=config.get('authMechanism'),
        )
        self.db = config['db']
        self.table = config.get('table')

    def count(self):
        return self.mongo_client[self.db][self.table].count_documents({})

    def find(self, offset=0, limit=10):
        return self.mongo_client[self.db][self.table].find({}).skip(offset).limit(limit)

    def find_one(self, offset=0):
        return self.mongo_client[self.db][self.table].find({}).skip(offset).limit(1) 

    def client(self):
        return self.mongo_client


if __name__ == '__main__':
    from datetime import datetime, timedelta

    config = {
            'host': '106.14.94.38',
            'port': 27071,
            'db': 'saas_dq_uat',
            'table': 'pos'
    }
    mongo = Mongo(config=config).find(0, 1)
    for data in mongo:
        print(data)
        print('-' * 100)
