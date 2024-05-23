# 使用 mysql-binlog-connector 监听处理 MySQLBinlog 文件

## 1. 需求概述
   业务开发中经常需要根据一些数据变更实现相对应的操作。例如，一些用户注销自己的账户，系统可以给用户自动发短信确认，这时有两种解决方案，一种是耦合到业务系统中，当用户执行注销操作的时候，执行发短信的操作，既是是通过MQ也是要耦合业务代码的，第二种方案基于数据库层面的操作，通过监听binlog实现自动发短信操作，这样就可以与业务系统解耦。
   本示例主要基于mysql-binlog-connector实现对数据库的监听，并集成springboot的方案。

## 2. 技术选型
   基于binlog实现数据同步的方案有两种：
   一种是mysql-binlog-connector，另一种是ali的canal。
   mysql-binlog-connector：是通过引入依赖jar包实现，需要自行实现解析，但是相对轻量。
   canal：是数据同步中间件，需要单独部署维护，功能强大，支持数据库及MQ的同步，维护成本高。
   根据实际业务场景，按需索取，业务量小，业务简单，轻量可以通过mysql-binlog-connector，业务量大，逻辑复杂，有专门的运维团队，可以考虑canal，比较经过阿里高并发验证，相对稳定。

## 3. 方案设计
   1.支持对不同数据库，不同表的配置监听。
   2.封装细节数据库，对外提供统一监听。
   3.讲结果集封装位方便操作数据结构。
   5.讲监听信息统一放入阻塞队列。
   6.实现多线程消费。

## 4. 配置验证

1、正常开启状态

```bash
mysql> show variables like 'log_bin';
+---------------+-------+
| Variable_name | Value |
+---------------+-------+
| log_bin       | ON    |
+---------------+-------+
1 row in set (0.02 sec)
mysql> show binary logs;
+------------------+-----------+
| Log_name         | File_size |
+------------------+-----------+
| mysql-bin.000001 |       154 |
+------------------+-----------+
1 row in set (0.09 sec)
```

2、权限不足情况

```bash
mysql> show binary logs;
1227 - Access denied; you need (at least one of) the SUPER, REPLICATION CLIENT privilege(s) for this operation
```

3、未开启状态(默认情况下是不开启的)

```bash
mysql> show binary logs;
ERROR 1381 - You are not using binary logging
```

## 5. 代码示例

（详见工程代码）

### 5.1. 事件中断问题

当binlog服务上线之后，服务默认会在log文件的最新position处进行监听。格式如下：

```bash
Connected to 192.168.10.220:3306 at test-bin.000557/270069874 (sid:65535, cid:1089065)
```

> 其中 `test-bin.000557` 是磁盘上的 binlog 文件名称，`270069874` 是对应该 binlog 文件中的 position 位置。

但由于binlog服务下线，重新启动后，默认又开始在最新position处进行监听，会丢失一不分binlog的事件，所以每次事件均需要记录当前的position位置。
当重新启动服务时，使用记录的position位置初始化BinaryLogClient。

> 注：单实例可以使用记录到本地文件的方式存储，如果需要考虑多实例分布式问题则需要考虑存储到redis等共享存储中。因为事件属于高频操作，所以建议使用redis或者MQ这种写入速度较快的存储方式。

### 5.2 文件轮换问题

当有人发出一个`FLUSH LOGS`语句或者当前二进制日志文件变大超过`max_binlog_size`时，binlog的文件名会重新一个新的文件，所以我们也要监听`ROTATE`事件并进行binlogFileName的记录。

> 记录binlogFileName和Position的代码详见文件 `SaveBinlogFilenameAndPositionListener.java`

## 6. 单点问题

`mysql-binlog-connector` 监听只能是单点服务，但是实际业务中我们需要保证高可用，即某台机器挂掉不能影响业务。

实现思路：部署多台机器，只有一台机器能监听binlog，当这台机器挂掉后，其他机器抢夺binlog的监听。可以借助Zookeeper、Redis等方式实现。

