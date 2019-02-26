import os, sys, datetime, math
sys.path.append(os.getcwd())

from configparser import (ConfigParser, RawConfigParser, )

from utils.mongo import Mongo
from utils.format import (date_to_str, )


# 读取 config.ini 配置项
def read_config():
    cfg = ConfigParser()
    cfg.read('config/config.ini')

    mongo_conf = {
        'host': cfg.get('mongo', 'host'),
        'port': cfg.getint('mongo', 'port'),
        'db': cfg.get('mongo', 'db'),
        'table': cfg.get('mongo', 'table'),
    }
    elastic_conf = {
        'init': cfg.get('elastic', 'init'),
        'host': cfg.get('elastic', 'host'),
        'port': cfg.getint('elastic', 'port'),
        'index': cfg.get('elastic', 'index'),
        'type': cfg.get('elastic', 'type'),
    }
    
    is_null = lambda x : None if not x else x
    is_list = lambda x : None if not x else eval(x)

    return {'mongo': mongo_conf, 'elastic': elastic_conf}


# 写入 config.ini 配置项
def write_config(section, key, value):
    cfg = RawConfigParser()
    cfg.read('config/config.ini')
    if section not in cfg.sections():
        cfg.add_section(section)

    cfg.set(section, key, value)

    with open('config/config.ini', 'w') as f:
        cfg.write(f)


#读取 mapping 文件
def read_mapping(name):
    with open(name, 'r') as file:
        return file.read()
    return ''


# 初始化 elastic doc types
def init_elastic(flag):
    if 'True' == flag:
        # 修改初始化为 False
        write_config('elastic', 'init', 'False')
        # 执行初始化命令
        command = '''self._elastic(doc={}, option="init")'''.format(read_mapping('mapping/pos.json'))
        return command
    else:
        return '1 + 1'


# 业务相关方法：递归格式化 pos 
def format_pos(data):
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
                format_pos(v)
            elif type(v) is list:
                for i in v:
                    format_pos(i)
