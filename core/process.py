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

            # 业务相关操作：聚合 
            if k == 'terminal_open_time' and not 'terminal_open_time_dict' in old_mapping:
                old_mapping['terminal_open_time_dict'] = {
                    "type": "nested",
                    "properties": {"month": {"type": "keyword"},
                    "day": {"type": "keyword"},
                    "hour": {"type": "keyword"}}
                }
            if k == 'store_branch' and not 'store_branch_dict' in old_mapping and len(v):
                old_mapping['store_branch_dict'] = {
                    "type": "nested",
                    "properties": { i+1: {"type": "keyword"} for i in range(len(v))}
                }
            if k == 'store_geo' and not 'store_geo_dict' in old_mapping and len(v):
                old_mapping['store_geo_dict'] = {
                    "type": "nested",
                    "properties": { i+1: {"type": "keyword"} for i in range(len(v))}
                }

            if not v:
               pass 
            elif k.endswith(('no', 'code', 'id', 'type')):
                old_mapping[k] = {"type": "keyword"}
            elif type(v) is int and old_mapping[k]['type'] in ['text', 'keyword']:
                old_mapping[k] = {"type": "long"}
            elif type(v) is bool and old_mapping[k]['type'] in ['text', 'keyword']:
                old_mapping[k] = {"type": "boolean"}
            elif type(v) is float and old_mapping[k]['type'] in ['text', 'keyword', 'long']:
                old_mapping[k] = {"type": "double"}
            elif type(v) is datetime.datetime and not old_mapping[k]['type'] is 'nested':
                old_mapping[k] = {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"}
            elif type(v) is str and (re.compile(r'....-..-.. ..:..:..').match(v) or re.compile(r'....-..-..').match(v)):
                old_mapping[k] = {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"}
            elif type(v) is list:
                first = v[0]
                if type(first) is dict:
                    if not k in old_mapping or not old_mapping[k]['type'] is 'nested':
                        old_mapping[k] = {"type": "nested", "properties": {}}
                    format_mapping(old_mapping[k]['properties'], first)
                else:
                    old_mapping[k] = {"type": "text"}
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
                    if type(i) is dict:
                        format_data(i)

# 业务相关操作：时段聚合
def format_data_for_aggs(data):
    if not data.get('terminal_open_time'):
        data['terminal_open_time_dict'] = None
    else:
        m_d_h = re.compile(r'....-(..)-(..) (..):.*?').findall(data['terminal_open_time'])[0]
        data['terminal_open_time_dict'] = {'month': m_d_h[0], 'day': m_d_h[1], 'hour': m_d_h[2]}
    if not data.get('store_branch'):
        data['store_branch_dict'] = None
    else:
        data['store_branch_dict'] = { i+1: v['id'] for i,v in enumerate(data['store_branch']) if v}
    if not data.get('store_geo'):
        data['store_geo_dict'] = None
    else:
        data['store_geo_dict'] = { i+1: v['id'] for i,v in enumerate(data['store_geo']) if v}
