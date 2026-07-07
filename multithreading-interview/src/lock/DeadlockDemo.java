package lock;

import java.util.concurrent.TimeUnit;

/**
 * 死锁 Demo：先制造死锁，再用「固定顺序加锁」避免死锁。
 *
 * 面试考点：
 *  1. 死锁四个必要条件：互斥、持有并等待、不可剥夺、循环等待。
 *  2. 破坏「循环等待」：所有线程按同一全局顺序申请锁。
 *  3. 排查：jps 找 pid，jstack 打印会显示 "Found one Java-level deadlock"。
 *
 * 运行：
 *   java src/lock/DeadlockDemo.java          # 默认演示"安全"版本（可正常结束）
 *   java src/lock/DeadlockDemo.java deadlock # 演示死锁（会卡住，需 Ctrl+C 或看 jstack）
 */
public class DeadlockDemo {

    private static final Object LOCK_A = new Object();
    private static final Object LOCK_B = new Object();

    public static void main(String[] args) throws InterruptedException {
        boolean showDeadlock = args.length > 0 && "deadlock".equalsIgnoreCase(args[0]);
        if (showDeadlock) {
            System.out.println("=== 死锁演示：两个线程反向加锁，将互相等待（用 jstack 观察）===");
            runDeadlock();
        } else {
            System.out.println("=== 安全演示：两个线程按相同顺序加锁，不会死锁 ===");
            runSafe();
        }
    }

    /** 反向加锁：t1 先 A 后 B，t2 先 B 后 A，极易死锁。 */
    private static void runDeadlock() {
        Thread t1 = new Thread(() -> {
            synchronized (LOCK_A) {
                System.out.println("t1 拿到 A，尝试拿 B ...");
                sleep(200);
                synchronized (LOCK_B) {
                    System.out.println("t1 拿到 A + B");
                }
            }
        }, "t1");

        Thread t2 = new Thread(() -> {
            synchronized (LOCK_B) {
                System.out.println("t2 拿到 B，尝试拿 A ...");
                sleep(200);
                synchronized (LOCK_A) {
                    System.out.println("t2 拿到 B + A");
                }
            }
        }, "t2");

        t1.start();
        t2.start();
    }

    /** 固定顺序：两个线程都「先 A 后 B」，破坏循环等待，不会死锁。 */
    private static void runSafe() throws InterruptedException {
        Runnable task = () -> {
            synchronized (LOCK_A) {
                sleep(100);
                synchronized (LOCK_B) {
                    System.out.println(Thread.currentThread().getName() + " 顺利拿到 A + B");
                }
            }
        };
        Thread t1 = new Thread(task, "t1");
        Thread t2 = new Thread(task, "t2");
        t1.start();
        t2.start();
        t1.join();
        t2.join();
        System.out.println("两个线程都正常结束，无死锁。");
    }

    private static void sleep(long ms) {
        try {
            TimeUnit.MILLISECONDS.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
