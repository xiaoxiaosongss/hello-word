package forkjoin;

import java.util.Arrays;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Fork/Join 并行归并排序 Demo（RecursiveAction 无返回值）。
 *
 * 面试考点：
 *  1. RecursiveAction 用于无返回值的分治任务（原地修改数组）。
 *  2. invokeAll(left, right) 一次性提交两个子任务并等待完成。
 *  3. 归并排序天然分治，非常契合 Fork/Join。
 *
 * 运行：java src/forkjoin/ForkJoinMergeSortDemo.java
 */
public class ForkJoinMergeSortDemo {

    private static final int THRESHOLD = 4_096;

    static class MergeSortAction extends RecursiveAction {
        private final int[] arr;
        private final int lo;
        private final int hi;

        MergeSortAction(int[] arr, int lo, int hi) {
            this.arr = arr;
            this.lo = lo;
            this.hi = hi;
        }

        @Override
        protected void compute() {
            if (hi - lo <= THRESHOLD) {
                Arrays.sort(arr, lo, hi); // 小段直接用双轴快排
                return;
            }
            int mid = (lo + hi) >>> 1;
            MergeSortAction left = new MergeSortAction(arr, lo, mid);
            MergeSortAction right = new MergeSortAction(arr, mid, hi);
            invokeAll(left, right); // 并行执行两个子任务并等待
            merge(arr, lo, mid, hi);
        }

        private void merge(int[] a, int lo, int mid, int hi) {
            int[] tmp = new int[hi - lo];
            int i = lo, j = mid, k = 0;
            while (i < mid && j < hi) {
                tmp[k++] = (a[i] <= a[j]) ? a[i++] : a[j++];
            }
            while (i < mid) tmp[k++] = a[i++];
            while (j < hi) tmp[k++] = a[j++];
            System.arraycopy(tmp, 0, a, lo, tmp.length);
        }
    }

    public static void main(String[] args) {
        int n = 20_000_000;
        int[] arr = new int[n];
        for (int i = 0; i < n; i++) {
            arr[i] = ThreadLocalRandom.current().nextInt();
        }
        int[] copy = arr.clone();

        long t1 = System.currentTimeMillis();
        Arrays.sort(copy); // 单线程基准
        long singleCost = System.currentTimeMillis() - t1;

        ForkJoinPool pool = new ForkJoinPool();
        long t2 = System.currentTimeMillis();
        pool.invoke(new MergeSortAction(arr, 0, n));
        long parallelCost = System.currentTimeMillis() - t2;
        pool.shutdown();

        System.out.println("元素个数                : " + n);
        System.out.println("Arrays.sort 单线程(ms)  : " + singleCost);
        System.out.println("ForkJoin 并行归并(ms)   : " + parallelCost);
        System.out.println("排序结果是否正确        : " + Arrays.equals(arr, copy));
    }
}
