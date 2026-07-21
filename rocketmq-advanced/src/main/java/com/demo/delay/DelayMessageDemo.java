package com.demo.delay;

import com.demo.RocketMqConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * delayTimeLevel=3 对应约 10 秒延迟（Broker 内置 18 级延迟表）。
 */
public class DelayMessageDemo {

  public static void main(String[] args) throws Exception {
    String group = "demo_delay_group";
    long sendAt = System.currentTimeMillis();

    CountDownLatch latch = new CountDownLatch(1);
    DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
    consumer.setNamesrvAddr(RocketMqConfig.NAMESRV_ADDR);
    consumer.subscribe(RocketMqConfig.TOPIC_DELAY, "*");
    consumer.registerMessageListener((MessageListenerConcurrently) (msgs, ctx) -> {
      msgs.forEach(msg -> {
        long delayMs = System.currentTimeMillis() - sendAt;
        System.out.printf("[Consumer] delay message received after ~%d ms: %s%n",
            delayMs, new String(msg.getBody(), StandardCharsets.UTF_8));
        latch.countDown();
      });
      return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    });
    consumer.start();

    DefaultMQProducer producer = new DefaultMQProducer("demo_delay_producer");
    producer.setNamesrvAddr(RocketMqConfig.NAMESRV_ADDR);
    producer.start();

    Message msg = new Message(RocketMqConfig.TOPIC_DELAY, "delay",
        "hello-delay".getBytes(StandardCharsets.UTF_8));
    msg.setDelayTimeLevel(3); // level 3 ≈ 10s
    producer.send(msg);
    System.out.println("[Producer] delay message sent at " + sendAt + " (level=3, ~10s)");

    boolean received = latch.await(20, TimeUnit.SECONDS);
    System.out.println(received ? "[Demo] delay delivery OK" : "[Demo] timeout (is Broker running?)");

    producer.shutdown();
    consumer.shutdown();
  }
}
