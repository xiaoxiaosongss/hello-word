package cas;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * 并发计数三种方式对比：synchronized / AtomicLong(CAS) / LongAdder。
 *
 * 面试考点：
 *  1. i++ 非原子（读-改-写），裸 long 在多线程下会丢更新。
 *  2. AtomicLong 基于 CAS 无锁，竞争激烈时自旋开销大。
 *  3. LongAdder 分段（Cell 数组）降低竞争，高并发写性能最好。
 *
 * 运行：java src/cas/CasCounterDemo.java
 */
public class CasCounterDemo {

    private static final int THREADS = 16;
    private static final int LOOPS = 1_000_000;
    private static final long EXPECTED = (long) THREADS * LOOPS;

    // 非线程安全的裸计数
    private static long unsafeCounter = 0;

    public static void main(String[] args) throws InterruptedException {
        // 1) 无同步（错误示范，结果通常 < EXPECTED）
        runUnsafe();

        // 2) synchronized
        Object lock = new Object();
        long[] syncBox = {0};
        long syncCost = run(() -> {
            synchronized (lock) {
                syncBox[0]++;
            }
        });
        System.out.printf("synchronized : 结果=%d 正确=%b 耗时=%dms%n",
                syncBox[0], syncBox[0] == EXPECTED, syncCost);

        // 3) AtomicLong（CAS）
        AtomicLong atomic = new AtomicLong();
        long atomicCost = run(atomic::incrementAndGet);
        System.out.printf("AtomicLong   : 结果=%d 正确=%b 耗时=%dms%n",
                atomic.get(), atomic.get() == EXPECTED, atomicCost);

        // 4) LongAdder
        LongAdder adder = new LongAdder();
        long adderCost = run(adder::increment);
        System.out.printf("LongAdder    : 结果=%d 正确=%b 耗时=%dms%n",
                adder.sum(), adder.sum() == EXPECTED, adderCost);
    }

    private static void runUnsafe() throws InterruptedException {
        unsafeCounter = 0;
        run(() -> unsafeCounter++);
        System.out.printf("裸 long(错误): 结果=%d 期望=%d 丢失=%d%n",
                unsafeCounter, EXPECTED, EXPECTED - unsafeCounter);
    }

    /** 启动 THREADS 个线程，每个执行 LOOPS 次 action，返回耗时(ms)。 */
    private static long run(Runnable action) throws InterruptedException {
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(THREADS);
        for (int i = 0; i < THREADS; i++) {
            new Thread(() -> {
                try {
                    start.await();
                    for (int j = 0; j < LOOPS; j++) {
                        action.run();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            }).start();
        }
        long t = System.currentTimeMillis();
        start.countDown();  // 同时开跑
        done.await();
        return System.currentTimeMillis() - t;
    }
}
