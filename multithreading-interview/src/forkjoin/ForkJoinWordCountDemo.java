package forkjoin;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

/**
 * Fork/Join 并行词频统计 Demo（RecursiveTask 返回 Map + 合并）。
 *
 * 演示：
 *  1. 子任务返回中间结果（Map），父任务负责合并（reduce）。
 *  2. 打印工作线程名，直观感受多个 ForkJoinPool 线程 + 工作窃取。
 *
 * 运行：java src/forkjoin/ForkJoinWordCountDemo.java
 */
public class ForkJoinWordCountDemo {

    private static final int THRESHOLD = 500;

    static class WordCountTask extends RecursiveTask<Map<String, Integer>> {
        private final String[] words;
        private final int lo;
        private final int hi;

        WordCountTask(String[] words, int lo, int hi) {
            this.words = words;
            this.lo = lo;
            this.hi = hi;
        }

        @Override
        protected Map<String, Integer> compute() {
            if (hi - lo <= THRESHOLD) {
                Map<String, Integer> counter = new HashMap<>();
                for (int i = lo; i < hi; i++) {
                    counter.merge(words[i], 1, Integer::sum);
                }
                return counter;
            }
            int mid = (lo + hi) >>> 1;
            WordCountTask left = new WordCountTask(words, lo, mid);
            WordCountTask right = new WordCountTask(words, mid, hi);
            left.fork();
            Map<String, Integer> rightRes = right.compute();
            Map<String, Integer> leftRes = left.join();
            rightRes.forEach((k, v) -> leftRes.merge(k, v, Integer::sum));
            return leftRes;
        }
    }

    public static void main(String[] args) {
        String[] sample = {"java", "thread", "lock", "java", "pool", "thread", "java", "cas"};
        int total = 100_000;
        String[] words = new String[total];
        for (int i = 0; i < total; i++) {
            words[i] = sample[i % sample.length];
        }

        ForkJoinPool pool = new ForkJoinPool();
        Map<String, Integer> result = pool.invoke(new WordCountTask(words, 0, total));
        pool.shutdown();

        System.out.println("总词数: " + total);
        System.out.println("词频统计结果:");
        result.entrySet().stream()
                .sorted((a, b) -> b.getValue() - a.getValue())
                .forEach(e -> System.out.printf("  %-8s : %d%n", e.getKey(), e.getValue()));

        int sum = result.values().stream().mapToInt(Integer::intValue).sum();
        System.out.println("校验（各词频之和应等于总词数）: " + (sum == total));
    }
}
