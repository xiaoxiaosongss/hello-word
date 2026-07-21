# RocketMQ 进阶面试题与实战 Demo

面向 Java 后端工程师的 **RocketMQ 进阶**复习资料。内容包含**高频进阶面试题详解**（含答案）与**可运行的 Java 代码 Demo**（顺序消息、延迟消息、事务消息、消息过滤、存储模型概念演示等）。

## 目录

- [RocketMQ 进阶面试题详解（核心知识点 + 答案）](./RocketMQ进阶面试题详解.md)
  - 架构与核心组件（NameServer / Broker / Producer / Consumer）
  - 消息存储模型（CommitLog / ConsumeQueue / IndexFile）
  - 消息类型：普通、顺序、延迟、事务
  - 消息可靠性（刷盘、主从复制、消费确认）
  - 消费者负载均衡与 Rebalance
  - 消息过滤（Tag / SQL92）
  - 重试队列与死信队列
  - 高可用（主从、DLedger、RocketMQ 5.x Proxy）
  - 性能调优与常见问题排查

## 环境要求

- JDK 17+
- Maven 3.8+
- Docker & Docker Compose（用于启动本地 RocketMQ）

## 快速开始

### 1. 启动 RocketMQ（Docker）

```bash
cd rocketmq-advanced
docker compose up -d
```

等待约 30 秒后，NameServer 监听 `9876`，Broker 监听 `10911`。

### 2. 编译项目

```bash
mvn -q compile
```

### 3. 运行 Demo

```bash
# 基础生产者 / 消费者
mvn -q exec:java -Dexec.mainClass="com.demo.basic.BasicProducerConsumerDemo"

# 顺序消息（同一 MessageQueue 内 FIFO）
mvn -q exec:java -Dexec.mainClass="com.demo.ordered.OrderedMessageDemo"

# 延迟消息（18 个延迟级别）
mvn -q exec:java -Dexec.mainClass="com.demo.delay.DelayMessageDemo"

# 事务消息（半消息 + 本地事务 + 回查）
mvn -q exec:java -Dexec.mainClass="com.demo.transaction.TransactionMessageDemo"

# Tag 过滤
mvn -q exec:java -Dexec.mainClass="com.demo.filter.TagFilterDemo"

# 重试队列 + 死信队列（需等待约 40s，maxReconsumeTimes=2）
mvn -q exec:java -Dexec.mainClass="com.demo.retry.RetryAndDlqDemo"

# 存储模型概念演示（无需 Broker，纯内存模拟）
mvn -q exec:java -Dexec.mainClass="com.demo.storage.StorageModelDemo"

# 或一键运行全部
bash run-all.sh
```

## Demo 列表

| 文件 | 演示知识点 |
| --- | --- |
| `src/main/java/com/demo/basic/BasicProducerConsumerDemo.java` | 同步/异步发送、集群消费、消费确认 |
| `src/main/java/com/demo/ordered/OrderedMessageDemo.java` | `MessageQueueSelector` 保证分区顺序 |
| `src/main/java/com/demo/delay/DelayMessageDemo.java` | `delayTimeLevel` 延迟投递 |
| `src/main/java/com/demo/transaction/TransactionMessageDemo.java` | 事务消息两阶段提交与回查 |
| `src/main/java/com/demo/filter/TagFilterDemo.java` | 生产者打 Tag、消费者 Tag 过滤 |
| `src/main/java/com/demo/retry/RetryAndDlqDemo.java` | `%RETRY%` 重试 → `%DLQ%` 死信、`setMaxReconsumeTimes` |
| `src/main/java/com/demo/storage/StorageModelDemo.java` | CommitLog + ConsumeQueue 写入/读取流程 |

## 停止环境

```bash
docker compose down
```
