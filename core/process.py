import os
import re
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
    try:
        with open('mapping/{}.json'.format(name), 'r') as file:
            return eval(file.read())
    except:
        return {"mappings":{name:{"properties": {}}}}


# 写入 mapping 文件
def write_mapping(name, data):
    with open('mapping/{}.json'.format(name), 'w') as file:
        json.dump(data, file)


# 生成mapping文件，根据mongo data
def format_mapping(old_mapping, new_data):
    '''
    name: mapping name
    old_mapping: 
    new_data: 
    '''
    if new_data:
        for k, v in new_data.items():
            if not k in old_mapping:
                old_mapping[k] = {"type": "text"}
            if not v:
               pass 
            elif type(v) is str:
                old_mapping[k] = {"type": "text"}
            elif type(v) is int and old_mapping[k]['type'] is 'text' and not re.compile(r'.*?_no').match(k) and not re.compile(r'.*?_id').match(k):
                old_mapping[k] = {"type": "long"}
            elif type(v) is bool and old_mapping[k]['type'] is 'text':
                old_mapping[k] = {"type": "boolean"}
            elif type(v) is float and old_mapping[k]['type'] in ['text', 'long']:
                old_mapping[k] = {"type": "double"}
            elif type(v) is datetime.datetime and not old_mapping[k]['type'] is 'nested':
                old_mapping[k] = {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"}
            elif type(v) is str and (re.compile(r'....-..-.. ..:..:..').match(v) or re.compile(r'....-..-..').match(v)):
                old_mapping[k] = {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"}
            elif type(v) is list:
                first = v[0]
                if not type(first) is dict:
                    old_mapping[k] = {"type": "text"}
                else:
                    if not (k in old_mapping and old_mapping[k]['type'] is 'nested'):
                        old_mapping[k] = {"type": "nested", "properties": {}}
                    format_mapping(old_mapping[k]['properties'], first)
            elif type(v) is dict:
                if not (k in old_mapping and old_mapping[k]['type'] is 'nested'):
                    old_mapping[k] = {"type": "nested", "properties": {}}
                format_mapping(old_mapping[k]['properties'], v)
            

# 递归格式化 mongo data
def format_data(data):
    if data:
        for k, v in data.items():
            if not v and type(v) in [str, list, dict]:
                data[k] = None
            elif type(v) is datetime.datetime:
                # 数据中时间类型转字符串类型
                data[k] = date_to_str(v)
            elif type(v) is float and math.isnan(v):
                data[k] = None
            elif type(v) is str and (v.upper() == 'NOUN' or v.upper() == 'MEAL_DEAL'):
                data[k] = None
            elif type(v) is dict:
                format_data(v)
            elif type(v) is list:
                for i in v:
                    format_data(i)
