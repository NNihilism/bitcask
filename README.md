# 项目介绍
该项目主要使用***Go***语言实现一个基于***batcask***的小型数据库。提供数据的增删查改功能，能够定期对归档文件进行整理。
## 使用说明
### C/S模式
##### 服务端的运行
进入目录文件
```
cd cmd/server
```
使用默认配置开启服务端
```
go run main.go
```
##### 客户端的运行
进入目录文件
```
cd cmd/client
```
使用默认配置开启客户端
```
go run client.go
```
### 代码内嵌(?)
参考examples下的代码
### 可提供功能
#### 数据操作
1. String
    - Set 插入/更新单条数据
    - Delete 删除数据
    - Get 获取数据
    - SetEX 插入有时效性的数据
    - SetNX 插入不存在的数据，key已存在时操作失败
    - MSet 同时插入多条记录
    - MSetNX
    - Append 在指定key对应的value的末尾添加数据，若key不存在则等同于Set
    - StrLen 返回指定key对应value的长度
    - Count 返回合法的记录条数
2. 其他
#### 其他
- 文件定期归档
- 数据的crc校验
## 实现思路
### 数据存储
#### 存储类型
目前只考虑了**string**类型的存储，后续会补充**set**、**map**等类型
#### 磁盘存储
使用文件进行数据的存储，需要注意的是，由于可能会有多个用户同时进行数据库的操作，因此需要设置文件锁来进行并发处理。  
不同的数据类型需要存储在不同的*logFile*中
将LogEntry记录到磁盘文件中时，使用了variant算法来存储entry的k/v长度，能以较小的字节存储较小的数字（自然数？）。
#### 内存存储
内存中存储的主要是一个前缀树，用来加速对记录的查找操作。分为***KeyValueMemMode***以及***KeyOnlyMemModel***两种模式，在第一个模式下，根据*key*能够直接读取到*value*，而使用后者时，只能根据*key*获取到对应的*logFileId*以及相应的*offset*,需要在磁盘中找到对应的*logFile*并读取*value*。
##### String
索引树中存储的节点格式如:  
- key----value  
##### List
由于List为双向链表，所以一颗索引树中存储了两种不同的节点。  
- Key---[LSeq, Rseq]用于维护Key对应列表的左右索引.  
- Seq+Key---Val,用于维护Key对应的列表元素以及该元素所在的索引.  
上述两种操作都会引发LogEntry的写入操作，且不区分写入的文件，即都会记录在log.List.xxxx文件中。
##### Hash
Hash与List相似，不同的Key存储在不同的索引树上，写入磁盘文件时，Key格式为Key+field以此区分不同的Key以及不同的field。
##### Set
去重：
- 对于一个索引树，树上的所有key本来就是唯一的，因此，如果不同的集合使用不同的索引树来维护，那么无需可以去重，只需要给集合的每个元素确定唯一的key即可，这里使用的是hash算法来生成唯一的key。
- 而在logfile中，每个entry写入时的key为其本身的key,而不是使用hash算法生成的key(可能是为了减少存储空间吧)，即不同的记录在磁盘文件中只需要区分所在的索引树即可，而不像Hash一样，还需要区分对应的field
- ##### ZSet
1 内存中同时维护索引树和跳表，磁盘中维护logfile.  
2 索引树中，key为member经过hash后的sum, value为member, 跟score无关  
3 跳表用来维护所需同一个key中的所有sum(hash后的member)和对应的score，方便获取指定范围以及排名等  


- 需要注意的是，删除操作时，参数没有score，因此在写入logentry时，其key-value与添加操作时写入的logentry的key-value不同，哪怕他们是对同一个member进行操作。写入操作时，logentry的key和value分别为key+score, member，这是因为在初始化索引时需要key,member,score等信息。删除操作时，key,value分别为key和sum(hash后的member),在建立索引时，遍历到该logentry,即可根据sum在索引树和跳表中删除之前建立的节点。
- zset的实现中，索引树有必要存在吗？ 好像没有...

