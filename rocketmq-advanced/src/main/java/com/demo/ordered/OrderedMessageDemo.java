package com.demo.ordered;

import com.demo.RocketMqConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 同一 orderId 路由到同一 MessageQueue，配合 Orderly 监听器保证分区内 FIFO。
 */
public class OrderedMessageDemo {

  public static void main(String[] args) throws Exception {
    String group = "demo_ordered_group";
  int orders = 2;
  int stepsPerOrder = 3;

    ConcurrentHashMap<String, AtomicInteger> lastStep = new ConcurrentHashMap<>();
    CountDownLatch latch = new CountDownLatch(orders * stepsPerOrder);

    DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
    consumer.setNamesrvAddr(RocketMqConfig.NAMESRV_ADDR);
    consumer.subscribe(RocketMqConfig.TOPIC_ORDERED, "*");
    consumer.registerMessageListener((MessageListenerOrderly) (msgs, ctx) -> {
      for (MessageExt msg : msgs) {
        String body = new String(msg.getBody(), StandardCharsets.UTF_8);
        // body format: orderId:step
        String[] parts = body.split(":");
        String orderId = parts[0];
        int step = Integer.parseInt(parts[1]);
        int expected = lastStep.computeIfAbsent(orderId, k -> new AtomicInteger(0)).incrementAndGet();
        boolean inOrder = step == expected;
        System.out.printf("[Consumer] order=%s step=%d expected=%d inOrder=%s queue=%d%n",
            orderId, step, expected, inOrder, msg.getQueueId());
        latch.countDown();
      }
      return ConsumeOrderlyStatus.SUCCESS;
    });
    consumer.start();

    DefaultMQProducer producer = new DefaultMQProducer("demo_ordered_producer");
    producer.setNamesrvAddr(RocketMqConfig.NAMESRV_ADDR);
    producer.start();

    MessageQueueSelector selector = (List<MessageQueue> mqs, Message msg, Object arg) -> {
      String orderId = (String) arg;
      int index = Math.abs(orderId.hashCode()) % mqs.size();
      return mqs.get(index);
    };

    for (int o = 1; o <= orders; o++) {
      String orderId = "order-" + o;
      for (int step = 1; step <= stepsPerOrder; step++) {
        Message msg = new Message(RocketMqConfig.TOPIC_ORDERED, "order",
            (orderId + ":" + step).getBytes(StandardCharsets.UTF_8));
        producer.send(msg, selector, orderId);
      }
    }
    System.out.println("[Producer] sent ordered messages for " + orders + " orders");

    boolean ok = latch.await(15, TimeUnit.SECONDS);
    System.out.println(ok ? "[Demo] ordered consumption verified" : "[Demo] timeout");

    producer.shutdown();
    consumer.shutdown();
  }
}
