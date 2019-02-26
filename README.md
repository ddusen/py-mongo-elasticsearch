# py-mongo-elasticsearch

### Overview

这是一个半成品的轮子，不能开箱即用...

| 文件 |    功能   | 描述 |
| :------| :-------- | :------ |
| config/config.ini |   配置        |  Mongo、ES 配置项  |
| core/sync.py   |  核心同步模块   | 封装了sql语句同步, es存储等功能 |
| core/process.py |  业务逻辑模块  |  处理业务逻辑，供核心同步模块调用，使用修改业务逻辑，一般就可以满足个人需求 |
| utils/\*       |  工具类        | 放了一些轮子会用到的工具类，比如：时间处理，字符串格式化等等 |
| mapping | ES文档结构 | 通过读取该目录的json，在es中自动创建index和mapping |
| logs | 日志 | 用来存放日志的 |
| requirements.txt | 项目依赖包 |  熟悉python的朋友都懂~ |


### Run

```
cd py-mongo-elasticsearch/

python3 -m venv venv
source venv/bin/activate
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple --upgrade pip
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

python core/sync.py
```

END!