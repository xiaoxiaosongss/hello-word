package threadpool;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ThreadPoolExecutor 七大参数 + 执行流程 + 拒绝策略 Demo。
 *
 * 面试考点：
 *  1. 七大参数含义。
 *  2. 执行流程：核心线程 -> 队列 -> 非核心线程 -> 拒绝策略。
 *  3. 阿里规约：手动 new ThreadPoolExecutor，使用有界队列，自定义线程名。
 *  4. CallerRunsPolicy：由提交任务的线程自己执行（反压不丢任务）。
 *
 * 运行：java src/threadpool/ThreadPoolDemo.java
 */
public class ThreadPoolDemo {

    static class NamedThreadFactory implements ThreadFactory {
        private final AtomicInteger idx = new AtomicInteger(1);

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "biz-pool-" + idx.getAndIncrement());
        }
    }

    public static void main(String[] args) throws InterruptedException {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                2,                                  // corePoolSize
                4,                                  // maximumPoolSize
                10, TimeUnit.SECONDS,               // keepAliveTime
                new ArrayBlockingQueue<>(2),        // 有界队列，容量 2
                new NamedThreadFactory(),           // 自定义线程工厂
                new ThreadPoolExecutor.CallerRunsPolicy() // 拒绝策略
        );

        // 总共提交 10 个任务：核心 2 + 队列 2 + 非核心到 max 2 = 最多同时容纳 6 个，
        // 其余触发 CallerRunsPolicy（由 main 线程执行）。
        for (int i = 1; i <= 10; i++) {
            final int taskId = i;
            executor.execute(() -> {
                System.out.printf("任务 %2d 由 [%s] 执行%n", taskId, Thread.currentThread().getName());
                sleep(500);
            });
            System.out.printf("提交任务 %2d 后 -> 活跃线程:%d 队列积压:%d 已完成:%d%n",
                    i, executor.getPoolSize(), executor.getQueue().size(), executor.getCompletedTaskCount());
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
        System.out.println("全部完成，共处理任务数: " + executor.getCompletedTaskCount());
    }

    private static void sleep(long ms) {
        try {
            TimeUnit.MILLISECONDS.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
