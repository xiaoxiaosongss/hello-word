# RocketMQ 进阶面试题详解

> 覆盖 RocketMQ **进阶 / 高频**考点，每题给出「面试怎么答 + 底层原理 + 加分项」。配套可运行 Demo 见 [`README.md`](./README.md)。

---

## 一、架构与核心组件

### 1. RocketMQ 整体架构是怎样的？

四大角色：

| 角色 | 职责 |
| --- | --- |
| **NameServer** | 轻量级注册中心，Broker 定时上报路由，Producer/Consumer 定时拉取 Topic 路由。无状态、可集群部署，节点间不通信。 |
| **Broker** | 消息存储与转发核心，接收 Producer 写入、向 Consumer 推送/拉取。支持 Master-Slave 部署。 |
| **Producer** | 消息生产者，通过 NameServer 获取路由后向 Broker 发消息。 |
| **Consumer** | 消息消费者，通过 NameServer 发现 Broker，以 Push 或 Pull 方式消费。 |

数据流：`Producer → Broker（CommitLog 落盘）→ Consumer`。

> 加分项：对比 Kafka —— RocketMQ 用 NameServer 而非 ZooKeeper/KRaft，更轻量；Broker 同时承担存储和协调，Topic 下分多个 MessageQueue（类似 Kafka Partition）。

### 2. NameServer 为什么设计成无状态？

- 各 NameServer 节点**互不通信**，Broker 向所有 NameServer 注册，客户端随机选一个 NameServer 拉路由，挂掉一个不影响整体。
- 路由信息存在内存，Broker 每 30s 心跳续约，NameServer 每 10s 扫描剔除超时 Broker。
- 优点：部署简单、无单点瓶颈；缺点：路由信息短暂不一致（最终一致），客户端需容忍。

### 3. Topic 和 MessageQueue 的关系？

- **Topic**：逻辑分类，如 `order_topic`。
- **MessageQueue**：Topic 的物理分区，一个 Topic 默认 4 个 Queue（可配置），消息按 Queue 存储，是**并行消费和顺序消息**的基本单位。
- 发送时通过 `MessageQueueSelector` 选择目标 Queue；消费时 Consumer Group 内各实例分摊 Queue（Rebalance）。

### 4. Producer Group 和 Consumer Group 的作用？

- **Producer Group**：事务消息回查时，Broker 需要找到同一 Group 的 Producer 实例做事务状态回查。
- **Consumer Group**：**负载均衡的单位**。同 Group 内多个 Consumer 分摊 Queue；不同 Group 各自独立消费全量消息（广播 vs 集群见下题）。

### 5. 广播消费 vs 集群消费？

| 模式 | 行为 | 典型场景 |
| --- | --- | --- |
| **集群（CLUSTERING）** | 同 Group 内每条消息只被一个实例消费 | 业务处理、削峰填谷（默认） |
| **广播（BROADCASTING）** | 同 Group 内每个实例都收到全量消息 | 本地缓存刷新、配置下发 |

集群模式下 Offset 存在 Broker（或消费进度服务端管理）；广播模式 Offset 存在 Consumer 本地文件。

---

## 二、消息存储模型（进阶核心）

### 6. RocketMQ 存储结构是怎样的？（CommitLog + ConsumeQueue + IndexFile）

这是 RocketMQ **最常被深挖**的考点：

```
Broker 存储目录
├── commitlog/          # 所有消息顺序写入同一个文件序列（核心）
├── consumequeue/       # 逻辑队列，存 offset + size + tagHash（定长 20 字节）
└── index/              # 按 key / 时间戳索引，加速按 key 查询
```

- **CommitLog**：所有 Topic 的消息**混写**在一个顺序文件中（默认 1GB 一个文件），充分利用顺序写磁盘性能。
- **ConsumeQueue**：每个 MessageQueue 对应一个 ConsumeQueue 文件，条目为 `(commitLogOffset, size, tagsCode)`，是 CommitLog 的**索引**，消费者读消息时先查 ConsumeQueue 再回 CommitLog 取 body。
- **IndexFile**：哈希索引，支持按 `keys` 或时间范围查询，非必须。

> 面试加分：Kafka 是每个 Partition 一个日志文件；RocketMQ 是**所有消息写 CommitLog + 逻辑队列索引**，写放大更小、顺序写更集中，但读路径多一次索引跳转。

### 7. 为什么 CommitLog 要混写而不是每个 Queue 一个文件？

1. **顺序写性能**：单文件追加写，充分利用 PageCache 和磁盘顺序 I/O。
2. **避免小文件过多**：Topic/Queue 数量大时，每 Queue 一个文件会产生大量随机 I/O 和 fd 开销。
3. ConsumeQueue 定长索引保证读路径仍然高效。

### 8. 刷盘机制：同步刷盘 vs 异步刷盘？

| 模式 | 行为 | 可靠性 | 性能 |
| --- | --- | --- | --- |
| **SYNC_FLUSH** | 消息写入 PageCache 后 `fsync` 落盘才返回 | 高（宕机不丢已确认消息） | 低 |
| **ASYNC_FLUSH** | 写入 PageCache 即返回，后台线程定时刷盘 | 可能丢少量未刷盘消息 | 高 |

生产环境常用：**异步刷盘 + 同步复制** 或 **同步刷盘 + 异步复制**，在性能和可靠性间折中。

### 9. 主从复制：SYNC_MASTER vs ASYNC_MASTER？

- **同步复制（SYNC_MASTER）**：Master 收到消息后等 Slave 同步成功才返回 Producer，Master 宕机不丢消息。
- **异步复制（ASYNC_MASTER）**：Master 写入后立即返回，Slave 异步拉取，Master 宕机可能丢消息。

配合刷盘策略形成四种组合，面试能说清 trade-off 即可。

---

## 三、消息类型

### 10. 普通消息的发送方式有哪些？

1. **同步发送**：`producer.send(msg)`，等待 Broker ACK，适合重要消息。
2. **异步发送**：`producer.send(msg, SendCallback)`，回调处理结果，高吞吐场景。
3. **单向发送**：`producer.sendOneway(msg)`，只管发不管结果，日志类场景。

### 11. 顺序消息如何保证？

RocketMQ 只保证 **同一个 MessageQueue 内 FIFO**，不保证跨 Queue 全局有序。

实现步骤：
1. 发送：实现 `MessageQueueSelector`，相同业务 key（如 orderId）路由到同一 Queue。
2. 消费：使用 `MessageListenerOrderly`（或 `ConsumeOrderlyContext`），Broker 对 Queue 加**分布式锁**，同一时刻只有一个线程消费该 Queue。

> 注意：消费失败会**暂停当前 Queue** 重试，阻塞该 Queue 后续消息，需控制重试次数避免「毒消息」卡死队列。

### 12. 延迟消息原理？

- 发送时设置 `msg.setDelayTimeLevel(n)`，不支持任意时间，只有 **18 个固定级别**（1s 5s 10s ... 2h）。
- Broker 收到后先写入 **Schedule Topic**（`SCHEDULE_TOPIC_XXXX`，按延迟级别分 18 个 Queue），由内置 `ScheduleMessageService` 定时扫描，到期后**重新写入目标 Topic** 供消费。
- RocketMQ 5.x 支持更灵活的定时/延迟（基于 Timer 机制），面试可提一嘴。

### 13. 事务消息流程？（高频）

解决**本地事务与发消息的原子性**问题（如扣库存 + 发通知）：

```
1. Producer 发送 Half Message（对消费者不可见）到 Broker
2. Broker 存储 Half Message，返回 ACK
3. Producer 执行本地事务（commit / rollback）
4. Producer 发送 Commit 或 Rollback 给 Broker
5. 若长时间未收到 4，Broker 回调 Producer 的 checkLocalTransaction 做回查
6. Commit 后消息对消费者可见；Rollback 则删除 Half Message
```

关键点：
- Half Message 存在特殊 Topic `RMQ_SYS_TRANS_HALF_TOPIC`
- 回查次数默认 15 次，超限丢弃
- 消费者端仍需做**幂等**（事务消息只保证「发或不发」，不保证「只消费一次」）

### 14. 批量消息与消息大小限制？

- 批量发送：`producer.send(Collection<Message>)`，单批建议 < 1MB。
- 默认单条消息最大 4MB（`maxMessageSize` 可配），生产建议单条 < 256KB，大文件走对象存储 + 消息传 URL。

---

## 四、消费模型

### 15. Push 和 Pull 的区别？RocketMQ 默认是什么？

| 维度 | Pull | Push |
| --- | --- | --- |
| 主动权 | Consumer 主动拉 | 表面是 Broker 推，底层仍是**长轮询 Pull** |
| 流控 | Consumer 自己控制速率 | `DefaultMQPushConsumer` 内部线程池 + 流控 |
| 使用 | `DefaultMQPullConsumer`（已不推荐） | `DefaultMQPushConsumer`（主流） |

> 加分项：RocketMQ 的 Push 本质是 `PullMessageService` 后台线程长轮询 Broker，有消息立即返回，无消息 hold 住连接（默认 15s），兼顾实时性和资源占用。

### 16. Rebalance（重平衡）是什么？什么时候触发？

Consumer Group 内各实例**重新分配 MessageQueue** 的过程。

触发时机：
- Consumer 上线 / 下线
- Topic 订阅变更
- Broker 上下线导致 Queue 数量变化

流程（简化）：
1. 每个 Consumer 向 Broker 申请锁（Rebalance 锁）
2. 从 NameServer 拉取最新路由
3. 按分配策略（默认平均分配 `AllocateMessageQueueAveragely`）计算自己负责的 Queue
4. 更新本地消费任务，对新 Queue 从上次 Offset 继续消费

> 面试坑：Rebalance 期间可能**重复消费**（旧实例未提交 Offset 新实例已接管），业务必须幂等。

### 17. Offset 存在哪里？如何管理？

- **集群模式**：Broker 端 `ConsumerOffset`（或 RocketMQ 5.x 统一管理），Consumer 定时上报消费进度。
- **广播模式**：Consumer 本地 `~/.rocketmq_offsets` 文件。
- 重置 Offset：`consumer.resetOffsetByTimestamp()` 或控制台按时间重置，用于回溯消费。

### 18. 消费失败后的重试机制？

1. 消费失败 → 消息发回 Broker **重试队列** `%RETRY%<ConsumerGroup>`。
2. 延迟级别递增重试（默认最多 16 次，间隔 10s 30s 1m ...）。
3. 超过最大重试次数 → 进入**死信队列（DLQ）** `%DLQ%<ConsumerGroup>`，需人工介入或单独消费。

---

## 五、消息过滤与查询

### 19. Tag 过滤 vs SQL92 过滤？

| 方式 | 用法 | 优缺点 |
| --- | --- | --- |
| **Tag** | 生产者 `msg.setTags("TagA")`，消费者 `subExpression = "TagA \|\| TagB"` | 简单高效，Broker 端过滤，减少网络传输 |
| **SQL92** | 用户属性 + SQL 表达式，如 `a > 5 AND b = 'vip'` | 灵活，但 Broker 需开启 `enablePropertyFilter`，有性能开销 |

过滤在 Broker 的 `ConsumeQueue` 层通过 `tagsCode` 或属性匹配完成，不匹配的消息不会发给 Consumer。

### 20. 如何实现消息幂等？

RocketMQ **至少一次（At Least Once）**语义，不保证恰好一次。幂等需在业务层实现：

1. **唯一键 + 数据库唯一索引**（订单号、消息 ID）
2. **Redis SETNX** 记录已处理 messageId
3. **状态机**：只允许从「待处理」→「已处理」一次流转

`msg.getMsgId()` 或业务 keys 均可作为幂等键。

---

## 六、高可用与 RocketMQ 5.x

### 21. Master-Slave 部署模式？

- **异步复制**：Master 写，Slave 异步同步，Master 宕机可能丢消息，可手动切换 Slave 为 Master（需运维介入）。
- **同步双写（Sync Master）**：Master 等 Slave ACK，可靠性高。
- **DLedger 模式**（4.x+）：基于 Raft 的多副本，自动选主，解决旧版主从切换不自动的问题。

### 22. RocketMQ 5.x 主要变化？（加分项）

- **Proxy 无状态网关**：客户端连 Proxy，Proxy 转 Broker，便于云原生、多语言（gRPC 协议）。
- **Pop 消费模式**：更轻量的消费协议，适合 Serverless。
- **更灵活的定时消息**：不再局限于 18 个延迟级别。
- **分层存储**：热数据本地盘，冷数据对象存储，降本。

### 23. RocketMQ vs Kafka 怎么选？

| 维度 | RocketMQ | Kafka |
| --- | --- | --- |
| 定位 | 业务消息、金融级可靠 | 大数据管道、日志采集 |
| 事务消息 | 原生支持 | 事务（0.11+）较重 |
| 延迟消息 | 内置 | 需自行实现 |
| 顺序消息 | Queue 级顺序，API 简单 | Partition 级顺序 |
| 吞吐 | 十万级/秒 | 百万级/秒 |
| 运维 | NameServer 轻量 | 依赖 ZK/KRaft |

---

## 七、性能调优与排查

### 24. Producer 端调优要点？

- 合理设置 `sendMsgTimeout`（默认 3s）
- 异步发送 + 批量（注意批量大小）
- 选择合适 `compressMsgBodyOverHowmuch` 开启压缩（默认 4KB 以上压缩）
- 失败重试 `retryTimesWhenSendFailed`（默认 2）
- 避免过大消息，大 payload 走 OSS

### 25. Consumer 端调优要点？

- `consumeThreadMin` / `consumeThreadMax`：消费线程池大小，CPU 密集 vs IO 密集区别对待
- `pullBatchSize`：单次拉取条数
- `consumeMessageBatchMaxSize`：批量消费回调条数
- 消费逻辑**异步化**耗时操作，避免阻塞消费线程导致 Rebalance
- 监控消费堆积（Consumer Lag）

### 26. 消息堆积怎么排查？

1. 看监控：哪个 Group、哪个 Queue 堆积
2. 消费速度 < 生产速度？→ 扩容 Consumer 实例（不超过 Queue 数）、优化消费逻辑
3. 消费卡死？→ 查毒消息、死锁、下游超时
4. Broker 磁盘 / IO 瓶颈？→ 扩容 Broker、异步刷盘、增加 Queue 数
5. 临时方案：跳过堆积（重置 Offset）、转储到备用 Topic 异步慢慢消费

### 27. 常见线上问题？

| 现象 | 可能原因 | 处理 |
| --- | --- | --- |
| 发送超时 | Broker 压力大、网络、同步刷盘+同步复制 | 调超时、检查 Broker 负载 |
| 重复消费 | Rebalance、消费失败重试、Producer 重试 | 业务幂等 |
| 顺序消息乱序 | 多线程消费同一 Queue、key 路由不一致 | MessageQueueSelector + Orderly 监听 |
| 事务消息悬挂 | 本地事务成功但 Commit 网络失败 | 依赖回查机制，保证回查幂等 |
| UNKNOWN_HOST | NameServer 地址错误或 Broker 未注册 | 检查 `NAMESRV_ADDR`、Broker 日志 |

---

## 八、快速记忆清单（面试前 5 分钟）

1. **架构**：NameServer 无状态路由；Broker 存消息；Push = 长轮询 Pull。
2. **存储**：CommitLog 混写 + ConsumeQueue 索引 + IndexFile 按 key 查。
3. **可靠**：刷盘（同步/异步）× 复制（同步/异步）四象限。
4. **顺序**：同 Queue + Orderly 消费 + 相同 key 路由。
5. **事务**：Half Message → 本地事务 → Commit/Rollback → 回查。
6. **延迟**：18 级 Schedule Topic 中转（5.x 更灵活）。
7. **消费**：集群分摊 Queue；失败 → 重试队列 → 死信队列。
8. **幂等**：业务自己做，MQ 只保证 At Least Once。
9. **Rebalance**：扩缩容时重新分 Queue，可能重复消费。
10. **5.x**：Proxy 网关、gRPC、Pop 消费、分层存储。

---

配套代码见 [`src/main/java/com/demo/`](./src/main/java/com/demo/) 目录。
