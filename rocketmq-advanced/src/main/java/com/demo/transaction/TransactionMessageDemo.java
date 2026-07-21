package com.demo.transaction;

import com.demo.RocketMqConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 事务消息：Half Message → 本地事务 → Commit/Rollback → 回查。
 */
public class TransactionMessageDemo {

  private static final Map<String, Boolean> LOCAL_TX = new ConcurrentHashMap<>();
  private static final AtomicInteger CHECK_COUNT = new AtomicInteger();

  public static void main(String[] args) throws Exception {
    String group = "demo_transaction_group";
    CountDownLatch latch = new CountDownLatch(1);

    DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
    consumer.setNamesrvAddr(RocketMqConfig.NAMESRV_ADDR);
    consumer.subscribe(RocketMqConfig.TOPIC_TRANSACTION, "*");
    consumer.registerMessageListener((MessageListenerConcurrently) (msgs, ctx) -> {
      msgs.forEach(msg -> {
        System.out.println("[Consumer] committed message: " + new String(msg.getBody(), StandardCharsets.UTF_8));
        latch.countDown();
      });
      return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    });
    consumer.start();

    TransactionMQProducer producer = new TransactionMQProducer("demo_transaction_producer");
    producer.setNamesrvAddr(RocketMqConfig.NAMESRV_ADDR);
    producer.setTransactionListener(new TransactionListener() {
      @Override
      public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        String txId = (String) arg;
        System.out.println("[Tx] execute local transaction: " + txId);
        LOCAL_TX.put(txId, true);
        return LocalTransactionState.COMMIT_MESSAGE;
      }

      @Override
      public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        String txId = msg.getTransactionId();
        CHECK_COUNT.incrementAndGet();
        boolean ok = LOCAL_TX.getOrDefault(txId, false);
        System.out.println("[Tx] check local transaction: " + txId + " => " + ok);
        return ok ? LocalTransactionState.COMMIT_MESSAGE : LocalTransactionState.ROLLBACK_MESSAGE;
      }
    });
    producer.start();

    String txId = "tx-" + System.currentTimeMillis();
    Message msg = new Message(RocketMqConfig.TOPIC_TRANSACTION, "tx",
        ("order-paid:" + txId).getBytes(StandardCharsets.UTF_8));
    producer.sendMessageInTransaction(msg, txId);
    System.out.println("[Producer] transaction message sent: " + txId);

    boolean consumed = latch.await(15, TimeUnit.SECONDS);
    System.out.println(consumed ? "[Demo] transaction message consumed" : "[Demo] timeout");
    System.out.println("[Demo] transaction check invocations: " + CHECK_COUNT.get());

    producer.shutdown();
    consumer.shutdown();
  }
}
