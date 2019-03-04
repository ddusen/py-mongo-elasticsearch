import os
import sys
import datetime
import math
import json
sys.path.append(os.getcwd())

from configparser import (ConfigParser, RawConfigParser, )

from utils.mongo import Mongo
from utils.format import (date_to_str, )


# 读取 config.ini 配置项
def read_config():
    cfg = ConfigParser()
    cfg.read('config/config.ini')

    is_null = lambda x: None if not x else x
    is_list = lambda x: None if not x else eval(x)

    mongo_conf = {
        'host': cfg.get('mongo', 'host'),
        'port': cfg.getint('mongo', 'port'),
        # 'user': cfg.get('mongo', 'user'),
        # 'passwd': cfg.get('mongo', 'passwd'),
        # 'authSource': cfg.get('mongo', 'authSource'),
        # 'authMechanism': cfg.get('mongo', 'authMechanism'),
        'db': cfg.get('mongo', 'db'),
        'tables': is_list(cfg.get('mongo', 'tables')),
    }
    elastic_conf = {
        'host': cfg.get('elastic', 'host'),
        'port': cfg.getint('elastic', 'port'),
    }
    oplog_conf = {
        'ts': is_null(cfg.get('oplog', 'ts')),
    }

    return {'mongo': mongo_conf, 'elastic': elastic_conf, 'oplog': oplog_conf}


# 写入 config.ini 配置项
def write_config(section, key, value):
    cfg = RawConfigParser()
    cfg.read('config/config.ini')
    if section not in cfg.sections():
        cfg.add_section(section)

    cfg.set(section, key, value)

    with open('config/config.ini', 'w') as f:
        cfg.write(f)


# 读取 mapping 文件
def read_mapping(name):
    mapping_name = 'mapping/{}.json'.format(name)
    try:
        with open(mapping_name, 'r') as file:
            return file.read()
    except:
        return None


# 写入 mapping 文件
def write_mapping(name, data):
    mapping_name = 'mapping/{}.json'.format(name)
    with open(mapping_name, 'w') as file:
        json.dump(data, file)


# 生成mapping文件，根据mongo data
def format_mapping(old_mapping, new_data):
    pass


# 业务相关方法：递归格式化 mongo data
def format_data(data):
    if data:
        for k, v in data.items():
            if not v:
                data[k] = None
            elif type(v) is datetime.datetime:
                # 数据中时间类型转字符串类型
                data[k] = date_to_str(v)
            elif type(v) is float and math.isnan(v):
                data[k] = None
            elif type(v) is dict:
                format_data(v)
            elif type(v) is list:
                for i in v:
                    format_data(i)
