package forkjoin;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Fork/Join 分治求和 Demo（RecursiveTask 有返回值）。
 *
 * 面试考点：
 *  1. 分治思想：大任务 fork 拆小，小任务 compute 直接算，再 join 合并。
 *  2. 拆分阈值 THRESHOLD 的取舍：太小调度开销大，太大并行度不足。
 *  3. "左 fork / 右 compute / join 左" 的写法减少一次线程调度。
 *  4. 底层的工作窃取（work-stealing）实现负载均衡。
 *
 * 运行：java src/forkjoin/ForkJoinSumDemo.java
 */
public class ForkJoinSumDemo {

    /** 小于该阈值就直接顺序计算，不再拆分。 */
    private static final int THRESHOLD = 10_000;

    static class SumTask extends RecursiveTask<Long> {
        private final long[] arr;
        private final int lo;
        private final int hi;

        SumTask(long[] arr, int lo, int hi) {
            this.arr = arr;
            this.lo = lo;
            this.hi = hi;
        }

        @Override
        protected Long compute() {
            int len = hi - lo;
            if (len <= THRESHOLD) {
                long sum = 0;
                for (int i = lo; i < hi; i++) {
                    sum += arr[i];
                }
                return sum;
            }
            int mid = (lo + hi) >>> 1;
            SumTask left = new SumTask(arr, lo, mid);
            SumTask right = new SumTask(arr, mid, hi);
            left.fork();                 // 异步：把左半任务放入当前线程的工作队列
            long rightResult = right.compute(); // 当前线程直接计算右半，避免多一次调度
            long leftResult = left.join();      // 等待左半结果（期间可能被其他线程窃取执行）
            return leftResult + rightResult;
        }
    }

    public static void main(String[] args) {
        int n = 50_000_000;
        long[] arr = new long[n];
        for (int i = 0; i < n; i++) {
            arr[i] = ThreadLocalRandom.current().nextInt(1, 100);
        }

        // 1) 单线程求和（基准）
        long t1 = System.currentTimeMillis();
        long single = 0;
        for (long v : arr) {
            single += v;
        }
        long singleCost = System.currentTimeMillis() - t1;

        // 2) Fork/Join 并行求和
        ForkJoinPool pool = new ForkJoinPool(); // 默认并行度 = CPU 核数
        long t2 = System.currentTimeMillis();
        long parallel = pool.invoke(new SumTask(arr, 0, n));
        long parallelCost = System.currentTimeMillis() - t2;
        pool.shutdown();

        System.out.println("CPU 核数 / 并行度      : " + Runtime.getRuntime().availableProcessors());
        System.out.println("元素个数              : " + n);
        System.out.println("单线程结果 / 耗时(ms) : " + single + " / " + singleCost);
        System.out.println("ForkJoin 结果 / 耗时  : " + parallel + " / " + parallelCost);
        System.out.println("结果是否一致          : " + (single == parallel));
    }
}
