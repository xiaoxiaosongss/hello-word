package com.demo.filter;

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
 * Tag 过滤：消费者 subExpression 只订阅 TagA 和 TagB。
 */
public class TagFilterDemo {

  public static void main(String[] args) throws Exception {
    String group = "demo_filter_group";
    CountDownLatch latch = new CountDownLatch(2);

    DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
    consumer.setNamesrvAddr(RocketMqConfig.NAMESRV_ADDR);
    consumer.subscribe(RocketMqConfig.TOPIC_FILTER, "TagA || TagB");
    consumer.registerMessageListener((MessageListenerConcurrently) (msgs, ctx) -> {
      msgs.forEach(msg -> {
        System.out.println("[Consumer] matched tag=" + msg.getTags() + " body="
            + new String(msg.getBody(), StandardCharsets.UTF_8));
        latch.countDown();
      });
      return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    });
    consumer.start();

    DefaultMQProducer producer = new DefaultMQProducer("demo_filter_producer");
    producer.setNamesrvAddr(RocketMqConfig.NAMESRV_ADDR);
    producer.start();

    producer.send(new Message(RocketMqConfig.TOPIC_FILTER, "TagA", "a".getBytes(StandardCharsets.UTF_8)));
    producer.send(new Message(RocketMqConfig.TOPIC_FILTER, "TagB", "b".getBytes(StandardCharsets.UTF_8)));
    producer.send(new Message(RocketMqConfig.TOPIC_FILTER, "TagC", "c-should-be-filtered".getBytes(StandardCharsets.UTF_8)));
    System.out.println("[Producer] sent TagA, TagB, TagC (TagC should be filtered)");

    boolean ok = latch.await(10, TimeUnit.SECONDS);
    System.out.println(ok ? "[Demo] tag filter OK (only TagA/TagB received)" : "[Demo] timeout");

    producer.shutdown();
    consumer.shutdown();
  }
}
