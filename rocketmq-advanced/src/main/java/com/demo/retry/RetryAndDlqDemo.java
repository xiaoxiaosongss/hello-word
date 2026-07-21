package com.demo.retry;

import com.demo.RocketMqConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 演示消费失败 → %RETRY%Group → 超过 maxReconsumeTimes → %DLQ%Group。
 *
 * <p>为加快本地演示，maxReconsumeTimes 设为 2（首次 + 2 次重试 = 最多 3 次消费后进 DLQ）。
 * 重试间隔由 Broker 固定（第 1 次 10s、第 2 次 30s），完整跑完约需 40s+。
 */
public class RetryAndDlqDemo {

  private static final int MAX_RECONSUME_TIMES = 2;
  private static final String DLQ_TOPIC = "%DLQ%" + RocketMqConfig.GROUP_RETRY;

  public static void main(String[] args) throws Exception {
    CountDownLatch dlqLatch = new CountDownLatch(1);
    AtomicInteger consumeAttempts = new AtomicInteger();

    // 1. 先启动 DLQ 消费者（独立 Group，订阅死信 Topic）
    DefaultMQPushConsumer dlqConsumer = new DefaultMQPushConsumer("demo_dlq_handler_group");
    dlqConsumer.setNamesrvAddr(RocketMqConfig.NAMESRV_ADDR);
    dlqConsumer.subscribe(DLQ_TOPIC, "*");
    dlqConsumer.registerMessageListener((MessageListenerConcurrently) (msgs, ctx) -> {
      for (MessageExt msg : msgs) {
        String body = new String(msg.getBody(), StandardCharsets.UTF_8);
        System.out.printf("[DLQ Consumer] received dead letter: body='%s', reconsumeTimes=%d, originTopic=%s%n",
            body, msg.getReconsumeTimes(), msg.getProperty("ORIGIN_MESSAGE_TOPIC"));
        dlqLatch.countDown();
      }
      return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    });
    dlqConsumer.start();
    System.out.println("[DLQ Consumer] listening on " + DLQ_TOPIC);

    // 2. 业务消费者：始终失败，触发重试
    DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RocketMqConfig.GROUP_RETRY);
    consumer.setNamesrvAddr(RocketMqConfig.NAMESRV_ADDR);
    consumer.setMaxReconsumeTimes(MAX_RECONSUME_TIMES);
    consumer.subscribe(RocketMqConfig.TOPIC_RETRY, "*");
    consumer.registerMessageListener((MessageListenerConcurrently) (msgs, ctx) -> {
      for (MessageExt msg : msgs) {
        int attempt = consumeAttempts.incrementAndGet();
        int reconsumeTimes = msg.getReconsumeTimes();
        String body = new String(msg.getBody(), StandardCharsets.UTF_8);
        System.out.printf("[Consumer] attempt=%d, reconsumeTimes=%d, body='%s' => RECONSUME_LATER%n",
            attempt, reconsumeTimes, body);
      }
      return ConsumeConcurrentlyStatus.RECONSUME_LATER;
    });
    consumer.start();
    System.out.printf("[Consumer] group=%s, maxReconsumeTimes=%d, retry topic=%%RETRY%%%s%n",
        RocketMqConfig.GROUP_RETRY, MAX_RECONSUME_TIMES, RocketMqConfig.GROUP_RETRY);

    // 3. 发送一条"毒消息"
    DefaultMQProducer producer = new DefaultMQProducer("demo_retry_producer");
    producer.setNamesrvAddr(RocketMqConfig.NAMESRV_ADDR);
    producer.start();
    Message msg = new Message(RocketMqConfig.TOPIC_RETRY, "retry",
        "poison-pill".getBytes(StandardCharsets.UTF_8));
    producer.send(msg);
    System.out.println("[Producer] sent poison message, waiting for retries then DLQ...");

    // 最多等 2 分钟（10s + 30s 重试间隔 + 缓冲）
    boolean inDlq = dlqLatch.await(120, TimeUnit.SECONDS);
    if (inDlq) {
      System.out.printf("[Demo] message entered DLQ after %d failed attempts (maxReconsumeTimes=%d)%n",
          consumeAttempts.get(), MAX_RECONSUME_TIMES);
    } else {
      System.out.println("[Demo] timeout — is Broker running? Retries need ~40s with maxReconsumeTimes=2");
    }

    producer.shutdown();
    consumer.shutdown();
    dlqConsumer.shutdown();
  }
}
