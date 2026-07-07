package queue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 生产者-消费者 Demo：基于 BlockingQueue（阻塞队列）实现。
 *
 * 面试考点：
 *  1. 阻塞队列 put/take 天然处理「满等待/空等待」，无需手写 wait/notify。
 *  2. 用「毒丸（poison pill）」优雅通知消费者退出。
 *  3. 对比 wait/notify 实现，阻塞队列更简洁不易错。
 *
 * 运行：java src/queue/ProducerConsumerDemo.java
 */
public class ProducerConsumerDemo {

    private static final int POISON = -1;      // 毒丸，标记生产结束
    private static final int PRODUCERS = 2;
    private static final int CONSUMERS = 3;
    private static final int ITEMS_PER_PRODUCER = 10;

    public static void main(String[] args) throws InterruptedException {
        BlockingQueue<Integer> queue = new ArrayBlockingQueue<>(8);
        AtomicInteger produced = new AtomicInteger();
        AtomicInteger consumed = new AtomicInteger();

        Thread[] producers = new Thread[PRODUCERS];
        for (int p = 0; p < PRODUCERS; p++) {
            final int pid = p;
            producers[p] = new Thread(() -> {
                try {
                    for (int i = 0; i < ITEMS_PER_PRODUCER; i++) {
                        int item = pid * 100 + i;
                        queue.put(item);           // 队列满则阻塞
                        produced.incrementAndGet();
                        System.out.printf("[生产者%d] 生产 %d%n", pid, item);
                        TimeUnit.MILLISECONDS.sleep(30);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "producer-" + p);
            producers[p].start();
        }

        Thread[] consumers = new Thread[CONSUMERS];
        for (int c = 0; c < CONSUMERS; c++) {
            final int cid = c;
            consumers[c] = new Thread(() -> {
                try {
                    while (true) {
                        int item = queue.take();   // 队列空则阻塞
                        if (item == POISON) {
                            break;                 // 收到毒丸，退出
                        }
                        consumed.incrementAndGet();
                        System.out.printf("            [消费者%d] 消费 %d%n", cid, item);
                        TimeUnit.MILLISECONDS.sleep(50);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "consumer-" + c);
            consumers[c].start();
        }

        // 等所有生产者完成，再投放与消费者等量的毒丸，让每个消费者都能退出
        for (Thread t : producers) {
            t.join();
        }
        for (int i = 0; i < CONSUMERS; i++) {
            queue.put(POISON);
        }
        for (Thread t : consumers) {
            t.join();
        }

        System.out.println("=== 完成 ===");
        System.out.println("生产总数: " + produced.get());
        System.out.println("消费总数: " + consumed.get());
        System.out.println("是否守恒: " + (produced.get() == consumed.get()));
    }
}
