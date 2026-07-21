package com.demo.basic;

import com.demo.RocketMqConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 演示同步发送、异步发送、单向发送，以及集群消费。
 */
public class BasicProducerConsumerDemo {

  public static void main(String[] args) throws Exception {
    String group = "demo_basic_group";
    String tag = "TagA";

    DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
    consumer.setNamesrvAddr(RocketMqConfig.NAMESRV_ADDR);
    consumer.subscribe(RocketMqConfig.TOPIC_BASIC, tag);
    CountDownLatch latch = new CountDownLatch(3);
    consumer.registerMessageListener((MessageListenerConcurrently) (msgs, ctx) -> {
      for (MessageExt msg : msgs) {
        String body = new String(msg.getBody(), StandardCharsets.UTF_8);
        System.out.println("[Consumer] received: " + body + ", queue=" + msg.getQueueId());
        latch.countDown();
      }
      return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    });
    consumer.start();
    System.out.println("[Consumer] started, group=" + group);

    DefaultMQProducer producer = new DefaultMQProducer("demo_basic_producer");
    producer.setNamesrvAddr(RocketMqConfig.NAMESRV_ADDR);
    producer.start();

    // 1. 同步发送
    Message syncMsg = new Message(RocketMqConfig.TOPIC_BASIC, tag, "sync-key",
        "sync-message".getBytes(StandardCharsets.UTF_8));
    SendResult syncResult = producer.send(syncMsg);
    System.out.println("[Producer] sync send OK: " + syncResult.getMsgId());

    // 2. 异步发送
    Message asyncMsg = new Message(RocketMqConfig.TOPIC_BASIC, tag, "async-key",
        "async-message".getBytes(StandardCharsets.UTF_8));
    CountDownLatch asyncLatch = new CountDownLatch(1);
    producer.send(asyncMsg, new SendCallback() {
      @Override
      public void onSuccess(SendResult sendResult) {
        System.out.println("[Producer] async send OK: " + sendResult.getMsgId());
        asyncLatch.countDown();
      }

      @Override
      public void onException(Throwable e) {
        System.err.println("[Producer] async send failed: " + e.getMessage());
        asyncLatch.countDown();
      }
    });
    asyncLatch.await(5, TimeUnit.SECONDS);

    // 3. 单向发送（日志场景，不关心结果）
    Message onewayMsg = new Message(RocketMqConfig.TOPIC_BASIC, tag, "oneway-key",
        "oneway-message".getBytes(StandardCharsets.UTF_8));
    producer.sendOneway(onewayMsg);
    System.out.println("[Producer] oneway send fired");

    boolean allConsumed = latch.await(10, TimeUnit.SECONDS);
    System.out.println(allConsumed ? "[Demo] all messages consumed" : "[Demo] timeout waiting for messages");

    producer.shutdown();
    consumer.shutdown();
  }
}
