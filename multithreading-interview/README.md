# Java 多线程 / 并发 面试题与实战 Demo

面向 Java 后端工程师的多线程 & 并发面试复习资料。内容包含**高频面试题详解**（含答案）与**可直接运行的 Java 代码 Demo**（Fork/Join、线程池、锁、CAS、CompletableFuture、生产者消费者等）。

## 目录

- [多线程面试题详解（核心知识点 + 答案）](./多线程面试题详解.md)
  - 线程基础、线程状态、创建方式
  - JMM（Java 内存模型）、`volatile`、`happens-before`
  - `synchronized` 原理、锁升级
  - AQS、`ReentrantLock`、`ReadWriteLock`
  - CAS 与原子类、ABA 问题
  - 线程池原理与参数、拒绝策略
  - **Fork/Join 框架（分治 + 工作窃取）**
  - `CompletableFuture` 异步编排
  - 并发容器：`ConcurrentHashMap`、阻塞队列
  - 死锁：产生条件、排查与避免
  - 常见并发工具：`CountDownLatch` / `CyclicBarrier` / `Semaphore`

## 可运行的代码 Demo

所有 Demo 均为**单文件、无第三方依赖**，需要 JDK 11+（推荐 JDK 17/21）。

```bash
cd multithreading-interview

# 直接用单文件源码启动模式运行（JDK 11+）
java src/forkjoin/ForkJoinSumDemo.java
java src/forkjoin/ForkJoinMergeSortDemo.java
java src/forkjoin/ForkJoinWordCountDemo.java
java src/threadpool/ThreadPoolDemo.java
java src/lock/DeadlockDemo.java
java src/cas/CasCounterDemo.java
java src/future/CompletableFutureDemo.java
java src/queue/ProducerConsumerDemo.java

# 或统一编译后运行
bash run-all.sh
```

## Demo 列表

| 文件 | 演示知识点 |
| --- | --- |
| `src/forkjoin/ForkJoinSumDemo.java` | `RecursiveTask` 分治求和，对比单线程 |
| `src/forkjoin/ForkJoinMergeSortDemo.java` | `RecursiveAction` 并行归并排序 |
| `src/forkjoin/ForkJoinWordCountDemo.java` | Fork/Join 统计词频，工作窃取 |
| `src/threadpool/ThreadPoolDemo.java` | `ThreadPoolExecutor` 七大参数、拒绝策略 |
| `src/lock/DeadlockDemo.java` | 制造死锁 + 顺序加锁避免死锁 |
| `src/cas/CasCounterDemo.java` | `synchronized` vs `AtomicLong` vs `LongAdder` |
| `src/future/CompletableFutureDemo.java` | 异步任务编排、组合、异常处理 |
| `src/queue/ProducerConsumerDemo.java` | 阻塞队列实现生产者消费者 |
