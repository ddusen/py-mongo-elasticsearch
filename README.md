# py-mongo-elasticsearch

### Overview

最近在做系统的改造，准备抛弃 Mongo，转用 ElasticSearch。要完成这项工作，首先就要考虑现有 mongo 的数据该如何同步到 es 中。我开始在网上寻找 mongo to es 的工具，找到了不少看似可以完成该工作的轮子，`elasticsearch-river-mongodb` `mongo-connector` `logstash-input-mongodb` 等等 

在一番研究后，我发现这些工具要么是太久没人维护，要么是open issue占总issue的80%，看到这种情况，我觉得我还是自己写一个吧，反正也不难😋

我的开发思路是这样的，先把mongo和es的配置放入配置文件 `config/config.ini`，然后程序 `core/init.py` 会读取配置文件中mongo的配置，根据配置文件中配置的需要同步的mongo表，程序会在mapping文件夹生成创建 es index和doc_type所需要的mapping文件 `mapping/*.json`。

为什么我不手动编写 es 的mapping呢？上次我根据mongo的一张表的结构建立mapping，没想到当前mongo库中的表结构如此冗长且复杂，而且结构还不固定。我最开始随便点了一条mongo的数据，然后手动编写mapping，花了一两个小时才完成一个表的mapping，看了一下mapping文件，真大，有700多行🤣好不容易建立好了mapping，结果发现还不能匹配所有数据，mongo中有一部分数据的结构不同，然后我就只能一边运行程序一边修改mapping，就这样用了一天时间修改了n多次，终于把那2w多条数据导入es。并且当时mongo表的结构层级又比较深，其中的酸爽就不多说了。在完成一个表mapping的建立后，我就决心一定要写一个程序，帮我自动识别mongo表结构，帮我自动创建mapping。这个程序的核心代码就在 `core/init.py` 

有了es mapping文件后，就可以开始把mongo中的数据同步到es中了。我把这个同步分为两块，一块是同步mongo历史所有数据，也是全量同步；另一块是同步mongo新增的数据，包括 新增、修改、删除，也就是增量同步。我编写了核心同步代码放在了 `core/sync.py` 中，全量同步基于mongo的查询语句，每次获取100条，然后放入es中，然后再重复执行该操作，直到历史数据同步完成。增量同步基于mongo的oplog，通过查询oplog，记录当前访问的timestamp，然后会一直轮询mongo，程序不会停止，中间有一些sleep，所以不会对mongo和es造成太大压力。

整个的同步逻辑就是这样的，在我看来这应该不算是一个成品的轮子，如果其他人借鉴的话，可能需要去读一读代码，好在代码量很少，核心代码总共也就小几百行，下面放一些文件的描述：

| 文件              | 功能         | 描述                                                         |
| :---------------- | :----------- | :----------------------------------------------------------- |
| config/config.ini | 配置         | Mongo、ES 配置项                                             |
| core/init.py      | 生成mapping  | 读取配置文件中需要同步的mongo表，生成符合规范的es mapping文件 |
| core/sync.py      | 核心同步模块 | 封装了sql语句同步, es存储等功能                              |
| core/process.py   | 业务逻辑模块 | 处理业务逻辑，供核心同步模块调用                             |
| utils/\*          | 工具类       | 放了一些轮子会用到的工具类，比如：时间处理，字符串格式化等等 |
| mapping           | ES文档结构   | 通过读取该目录的json，在es中自动创建index和mapping           |
| logs              | 日志         | 用来存放日志的                                               |
| requirements.txt  | 项目依赖包   | 熟悉python的朋友都懂~                                        |

### Run

```
cd py-mongo-elasticsearch/

python3 -m venv venv
source venv/bin/activate
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple --upgrade pip
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

python core/init.py
python core/sync.py
```

END!