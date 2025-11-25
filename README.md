# Flink生产级别学习项目

## 项目简介

这是一个完整的Flink生产级别学习项目，涵盖了Flink所有核心概念和高级特性。每个模块都包含详细的理论讲解和可运行的demo代码。

## 项目结构

```
flink/
├── src/main/java/com/kinch/flink/
│   ├── common/                    # 公共模块
│   │   ├── entity/               # 实体类
│   │   │   ├── SensorReading.java       # 传感器读数实体
│   │   │   ├── UserBehavior.java        # 用户行为实体
│   │   │   └── OrderEvent.java          # 订单事件实体
│   │   └── source/               # 自定义数据源
│   │       └── CustomSensorSource.java  # 自定义传感器数据源
│   │
│   ├── basics/                    # 基础API
│   │   └── DataStreamBasicsDemo.java    # DataStream基础操作
│   │
│   ├── window/                    # 窗口操作
│   │   └── WindowBasicsDemo.java        # 窗口基础演示
│   │
│   ├── watermark/                 # 水位线
│   │   └── WatermarkDemo.java           # 水位线演示
│   │
│   ├── state/                     # 状态管理
│   │   └── StateManagementDemo.java     # 状态管理演示
│   │
│   ├── checkpoint/                # Checkpoint和容错
│   │   └── CheckpointDemo.java          # Checkpoint演示
│   │
│   ├── processfunction/           # ProcessFunction
│   │   └── ProcessFunctionDemo.java     # ProcessFunction演示
│   │
│   ├── sideoutput/                # 侧输出流
│   │   └── SideOutputDemo.java          # 侧输出流演示
│   │
│   ├── asyncio/                   # 异步IO
│   │   └── AsyncIODemo.java             # 异步IO演示
│   │
│   ├── table/                     # Table API & SQL
│   │   └── TableAPIDemo.java            # Table API演示
│   │
│   ├── cep/                       # 复杂事件处理
│   │   └── CEPDemo.java                 # CEP演示
│   │
│   ├── connector/                 # 连接器
│   │   ├── KafkaConnectorDemo.java      # Kafka连接器
│   │   └── JDBCConnectorDemo.java       # JDBC连接器
│   │
│   └── serialization/             # 序列化
│       └── SerializationDemo.java       # 序列化演示
│
├── pom.xml                        # Maven配置
└── README.md                      # 项目说明
```

## 核心模块说明

### 1. 基础API (basics)

**DataStreamBasicsDemo.java**

- Map、Filter、FlatMap等基础转换操作
- KeyBy分组和聚合操作
- Union和Connect多流操作
- 并行度和算子链配置

**核心概念：**

- StreamExecutionEnvironment：执行环境
- DataStream：数据流抽象
- Transformation：数据转换
- Source和Sink：数据源和目标

### 2. 窗口操作 (window)

**WindowBasicsDemo.java**

- 滚动窗口（Tumbling Window）
- 滑动窗口（Sliding Window）
- 会话窗口（Session Window）
- 计数窗口（Count Window）
- 窗口函数：Reduce、Aggregate、Process

**核心概念：**

- Window Assigner：窗口分配器
- Trigger：触发器
- Evictor：驱逐器
- 增量聚合 vs 全量聚合

### 3. 水位线 (watermark)

**WatermarkDemo.java**

- 单调递增水位线
- 有界乱序水位线
- 自定义水位线生成器
- 延迟数据处理
- allowedLateness和侧输出流

**核心概念：**

- Watermark原理和传播
- 事件时间 vs 处理时间
- 乱序数据处理
- 窗口触发机制

### 4. 状态管理 (state)

**StateManagementDemo.java**

- ValueState：单值状态
- ListState：列表状态
- MapState：映射状态
- ReducingState：归约状态
- AggregatingState：聚合状态
- State TTL：状态生存时间

**核心概念：**

- Keyed State vs Operator State
- State Backend：内存、FsStateBackend、RocksDB
- 状态清理策略
- 状态访问性能优化

### 5. Checkpoint和容错 (checkpoint)

**CheckpointDemo.java**

- Checkpoint配置和调优
- StateBackend选择
- 重启策略配置
- Exactly-Once语义
- Savepoint使用

**核心概念：**

- Chandy-Lamport算法
- Barrier对齐机制
- 两阶段提交协议
- 故障恢复流程

### 6. ProcessFunction (processfunction)

**ProcessFunctionDemo.java**

- 温度持续上升检测
- 超时检测
- 温度异常检测
- 定时器使用
- 访问底层信息

**核心概念：**

- ProcessFunction vs 其他Function
- 定时器（Timer）机制
- Context上下文访问
- 事件时间 vs 处理时间定时器

### 7. 侧输出流 (sideoutput)

**SideOutputDemo.java**

- 数据分流
- 多级告警系统
- 数据质量监控
- 实时ETL

**核心概念：**

- OutputTag定义
- 侧输出 vs split/select
- 一次处理多路输出
- 不同类型的侧输出流

### 8. 异步IO (asyncio)

**AsyncIODemo.java**

- 异步数据库查询
- 异步HTTP请求
- 异步缓存查询
- 有序 vs 无序模式
- 超时处理

**核心概念：**

- 异步IO原理和优势
- RichAsyncFunction
- ResultFuture
- 并发控制和超时配置

### 9. Table API & SQL (table)

**TableAPIDemo.java**

- Table API基础操作
- SQL查询和聚合
- 窗口操作
- TopN查询
- Join操作
- DDL和Catalog

**核心概念：**

- 动态表（Dynamic Table）
- 时间属性
- Changelog模式
- 输出模式：Append/Retract/Upsert

### 10. 复杂事件处理 (cep)

**CEPDemo.java**

- 温度持续上升检测
- 订单超时检测
- 连续登录失败检测
- 传感器故障检测
- 模式匹配和量词

**核心概念：**

- Pattern定义
- 模式匹配策略：next/followedBy/followedByAny
- 量词：times/oneOrMore/optional
- 超时处理
- 匹配跳过策略

### 11. 连接器 (connector)

#### KafkaConnectorDemo.java

- KafkaSource配置和使用
- KafkaSink配置和使用
- Exactly-Once语义
- 动态分区发现
- 动态Topic路由
- 性能优化配置

#### JDBCConnectorDemo.java

- JdbcSink基础配置
- UPSERT模式
- 批量写入优化
- 连接池配置
- 错误处理

**核心概念：**

- Source和Sink
- 序列化和反序列化
- Exactly-Once实现
- 性能调优

### 12. 序列化 (serialization)

**SerializationDemo.java**

- POJO序列化
- JSON序列化
- CSV序列化
- Kryo配置
- 类型信息显式指定

**核心概念：**

- TypeInformation
- TypeSerializer
- POJO要求和规范
- Schema演化
- 性能对比

## 环境要求

### 1. 开发环境

- JDK 21
- Maven 3.6+
- IDE（推荐IntelliJ IDEA）

### 2. Docker环境（已配置）

- Flink 1.18.1
- JobManager和TaskManager已启动
- WebUI: http://localhost:8081

### 3. 可选依赖（用于完整功能）

- Kafka 2.4.0+（用于Kafka连接器示例）
- MySQL 8.0+（用于JDBC连接器示例）
- Redis 6.0+（用于异步IO示例）

## 快速开始

### 1. 编译项目

```bash
mvn clean package
```

### 2. 运行示例

#### 方式一：IDE直接运行

直接运行各个Demo类的main方法

#### 方式二：提交到Flink集群

```bash
# 编译打包
mvn clean package -DskipTests

# 提交任务
flink run -c com.kinch.flink.basics.DataStreamBasicsDemo \
  target/flink-0.0.1-SNAPSHOT.jar
```

#### 方式三：访问Flink WebUI

1. 打开浏览器访问：http://localhost:8081
2. 上传jar包
3. 指定入口类
4. 提交任务

## 学习路径建议

### 第一阶段：基础入门（1-2天）

1. DataStream基础API
2. 窗口操作
3. 时间和水位线

### 第二阶段：核心特性（2-3天）

4. 状态管理
5. Checkpoint和容错
6. ProcessFunction

### 第三阶段：高级特性（2-3天）

7. 侧输出流
8. 异步IO
9. Table API & SQL

### 第四阶段：实战应用（2-3天）

10. CEP复杂事件处理
11. 连接器（Kafka、JDBC）
12. 序列化优化

## 生产环境最佳实践

### 1. 性能优化

- 合理设置并行度
- 使用RocksDB StateBackend（大状态）
- 启用对象重用
- 调整网络缓冲区
- 使用异步IO

### 2. 可靠性保障

- 启用Checkpoint
- 配置合理的重启策略
- 使用Savepoint进行版本升级
- 监控状态大小
- 设置State TTL

### 3. 监控告警

- 监控Checkpoint时长和大小
- 监控反压（Backpressure）
- 监控消费Lag（Kafka）
- 设置合理的告警阈值

### 4. 资源配置

- 合理分配TaskManager资源
- 设置合理的Task Slots
- 配置JVM参数
- 使用资源隔离

### 5. 开发规范

- 使用POJO而非Tuple
- 避免在算子中创建重量级对象
- 及时清理状态
- 使用日志而非System.out

## 常见问题

### Q1: 如何选择窗口类型？

- 固定时间统计 → 滚动窗口
- 实时趋势分析 → 滑动窗口
- 用户会话分析 → 会话窗口
- 固定数量批处理 → 计数窗口

### Q2: 如何选择StateBackend？

- 状态 < 几百MB → HashMapStateBackend
- 状态 > 1GB → EmbeddedRocksDBStateBackend
- 需要增量Checkpoint → RocksDB

### Q3: Exactly-Once如何实现？

- Source：Checkpoint保存Offset
- 算子：Checkpoint保存State
- Sink：两阶段提交或幂等写入

### Q4: 如何处理数据倾斜？

- 添加随机前缀打散
- 两阶段聚合
- 使用KeyBy的变体

### Q5: 如何优化Checkpoint性能？

- 使用RocksDB增量Checkpoint
- 增加Checkpoint间隔
- 使用异步Checkpoint
- 减少状态大小

## 参考资料

### 官方文档

- [Apache Flink官方文档](https://flink.apache.org/docs/stable/)
- [Flink GitHub](https://github.com/apache/flink)

### 推荐书籍

- 《Flink原理、实战与性能优化》
- 《Stream Processing with Apache Flink》

### 社区资源

- [Flink中文社区](https://flink-learning.org.cn/)
- [Flink Forward大会](https://www.flink-forward.org/)

## 贡献指南

欢迎提交Issue和Pull Request！

## 许可证

本项目仅用于学习目的。

## 联系方式

如有问题，欢迎交流讨论。

---

**祝学习愉快，掌握Flink核心技术！**

