import pymysql

# db_config like this:
#
# {
#     'host':'127.0.0.1',
#     'port':3306
#     'user':'root',
#     'passwd':'123456',
#     'db':'mysql',
# }

class MySQL:
    def __init__(self, db_config):
        self.host = db_config.get('host')
        self.port = db_config.get('port')
        self.user = db_config.get('user')
        self.passwd = db_config.get('passwd')
        self.db = db_config.get('db')

    def open(self):
        return pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.passwd,
            db=self.db,
            charset='utf8')



def query(sql, db_config, list1=()):
    db = MySQL(db_config).open()
    cursor = db.cursor()
    cursor.execute(sql, list1)
    result = cursor.fetchall()

    db.commit()
    db.close()
    return result


def query_one(sql, db_config, list1=()):
    db = MySQL(db_config).open()
    cursor = db.cursor()
    cursor.execute(sql, list1)
    result = cursor.fetchone()

    db.commit()
    db.close()
    return result


def save(sql, db_config, list1=()):
    db = MySQL(db_config).open()
    cursor = db.cursor()
    try:
        result = cursor.execute(sql, list1)
        db.commit()
        return result
    except Exception as e:
        print (e)
        db.rollback()
        return None
    db.close()
