package future;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * CompletableFuture 异步编排 Demo。
 *
 * 面试考点：
 *  1. supplyAsync 提交异步任务（建议传自定义线程池，避免占满 commonPool）。
 *  2. thenCombine 合并两个并行任务的结果。
 *  3. thenApply / thenCompose 串行依赖。
 *  4. exceptionally / handle 异常处理，allOf 等待全部完成。
 *
 * 场景：并行查询「商品价格」和「用户优惠券」，合并算出应付金额。
 *
 * 运行：java src/future/CompletableFutureDemo.java
 */
public class CompletableFutureDemo {

    public static void main(String[] args) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(4);
        long start = System.currentTimeMillis();

        // 两个远程调用并行执行（各耗时 ~500ms）
        CompletableFuture<Double> priceFuture = CompletableFuture.supplyAsync(() -> {
            sleep(500);
            System.out.println("[" + Thread.currentThread().getName() + "] 查询商品价格 = 100.0");
            return 100.0;
        }, pool);

        CompletableFuture<Double> couponFuture = CompletableFuture.supplyAsync(() -> {
            sleep(500);
            System.out.println("[" + Thread.currentThread().getName() + "] 查询优惠券 = 20.0");
            return 20.0;
        }, pool);

        // 合并两个结果：应付 = 价格 - 优惠券
        CompletableFuture<Double> payFuture = priceFuture
                .thenCombine(couponFuture, (price, coupon) -> price - coupon)
                .thenApply(pay -> {
                    System.out.println("[" + Thread.currentThread().getName() + "] 计算应付金额");
                    return pay;
                })
                .exceptionally(ex -> {
                    System.out.println("发生异常，返回兜底价: " + ex.getMessage());
                    return 0.0;
                });

        double pay = payFuture.get(); // 阻塞获取最终结果
        long cost = System.currentTimeMillis() - start;
        System.out.printf("应付金额 = %.1f （并行耗时约 %dms，若串行需 ~1000ms）%n", pay, cost);

        // 演示异常传播与 handle
        CompletableFuture<String> risky = CompletableFuture
                .supplyAsync(() -> {
                    if (System.nanoTime() % 1 == 0) {
                        throw new IllegalStateException("库存服务超时");
                    }
                    return "ok";
                }, pool)
                .handle((res, ex) -> ex == null ? res : "降级默认库存");
        System.out.println("handle 结果: " + risky.get());

        pool.shutdown();
    }

    private static void sleep(long ms) {
        try {
            TimeUnit.MILLISECONDS.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
