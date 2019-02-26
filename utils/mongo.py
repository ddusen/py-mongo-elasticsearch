from pymongo import MongoClient

# config like this:
#
# {
#     'host':'127.0.0.1',
#     'port':3306
#     'user':'root',
#     'passwd':'123456'
# }
class Mongo:
    def __init__(self, config):
        self.client = MongoClient(
            config.get('host', 'localhost'),
            config.get('port', 27017),
            )
        self.db = config.get('db')
        self.table = config.get('table')

    def count(self):
        return self.client[self.db][self.table].count_documents({})

    def find(self, offset=0, limit=10):
        return self.client[self.db][self.table].find({}).skip(offset).limit(limit)


if __name__ == '__main__':
    mongo = Mongo(config={}).find('saas_dq_uat', 'pos', 1, 10)
    for data in mongo:
        print(data)
        print('-'*100)
