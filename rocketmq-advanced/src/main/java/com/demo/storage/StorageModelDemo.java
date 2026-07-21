package com.demo.storage;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 纯内存模拟 RocketMQ 存储模型：CommitLog 顺序写 + ConsumeQueue 定长索引。
 * 无需启动 Broker，帮助理解 CommitLog / ConsumeQueue 读写路径。
 */
public class StorageModelDemo {

  static class CommitLogEntry {
    final long offset;
    final byte[] body;

    CommitLogEntry(long offset, byte[] body) {
      this.offset = offset;
      this.body = body;
    }
  }

  static class ConsumeQueueEntry {
    final long commitLogOffset;
    final int size;
    final int tagHash;

    ConsumeQueueEntry(long commitLogOffset, int size, int tagHash) {
      this.commitLogOffset = commitLogOffset;
      this.size = size;
      this.tagHash = tagHash;
    }
  }

  public static void main(String[] args) {
    List<CommitLogEntry> commitLog = new ArrayList<>();
    Map<Integer, List<ConsumeQueueEntry>> consumeQueues = new LinkedHashMap<>();
    long nextOffset = 0;

    // 模拟 2 个 MessageQueue（queueId 0 和 1）
    consumeQueues.put(0, new ArrayList<>());
    consumeQueues.put(1, new ArrayList<>());

    String[][] messages = {
        {"0", "order-1001:created"},
        {"1", "order-1002:created"},
        {"0", "order-1001:paid"},
        {"1", "order-1002:paid"},
    };

    System.out.println("=== 写入阶段：所有消息顺序写入 CommitLog，同时写 ConsumeQueue 索引 ===");
    for (String[] item : messages) {
      int queueId = Integer.parseInt(item[0]);
      byte[] body = item[1].getBytes(StandardCharsets.UTF_8);
      int size = body.length;
      int tagHash = "order".hashCode();

      commitLog.add(new CommitLogEntry(nextOffset, body));
      consumeQueues.get(queueId).add(new ConsumeQueueEntry(nextOffset, size, tagHash));

      System.out.printf("  CommitLog offset=%d size=%d -> ConsumeQueue[%d]%n", nextOffset, size, queueId);
      nextOffset += size;
    }

    System.out.println();
    System.out.println("=== 读取阶段：Consumer 先读 ConsumeQueue，再按 offset 回 CommitLog 取 body ===");
    for (Map.Entry<Integer, List<ConsumeQueueEntry>> entry : consumeQueues.entrySet()) {
      int queueId = entry.getKey();
      System.out.println("Queue " + queueId + ":");
      for (ConsumeQueueEntry idx : entry.getValue()) {
        CommitLogEntry logEntry = commitLog.stream()
            .filter(e -> e.offset == idx.commitLogOffset)
            .findFirst()
            .orElseThrow();
        String body = new String(logEntry.body, StandardCharsets.UTF_8);
        System.out.printf("  index(offset=%d, size=%d) => body='%s'%n",
            idx.commitLogOffset, idx.size, body);
      }
    }

    System.out.println();
    System.out.println("[Demo] 要点：");
    System.out.println("  1. 所有 Topic/Queue 的消息混写在同一个 CommitLog（顺序写）");
    System.out.println("  2. ConsumeQueue 只存定长索引 (offset, size, tagHash)，读时二次查找 CommitLog");
    System.out.println("  3. 不同 Queue 可并行消费，同一 Queue 内按索引顺序读取");
  }
}
