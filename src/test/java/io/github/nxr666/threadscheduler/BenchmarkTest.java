package io.github.nxr666.threadscheduler;

import io.github.nxr666.threadscheduler.core.TaskManager;
import io.github.nxr666.threadscheduler.task.CallableTask;
import io.github.nxr666.threadscheduler.task.RunnableTask;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.text.DecimalFormat;

@Slf4j
@Tag("slow")
@SpringBootTest
public class BenchmarkTest {

    @Autowired
    private TaskManager taskManager;

    /**
     * 吞吐量基准测试
     */
    @Test
    public void benchmarkThroughput() throws InterruptedException {
        log.info("=== 吞吐量基准测试 ===");
        
        int[] taskCounts = {1000, 5000, 10000, 20000};
        
        DecimalFormat throughputDf = new DecimalFormat("#.##");
        for (int taskCount : taskCounts) {
            log.info("测试任务数量: {}", taskCount);
            
            // 预热
            warmUp(taskCount / 10);
            
            // 测试框架吞吐量
            long frameworkTime = measureThroughput(taskCount, "performance-executor");
            double frameworkThroughput = (taskCount * 1000.0) / frameworkTime; // 任务/秒
            
            // 测试传统线程池吞吐量
            long traditionalTime = measureTraditionalThroughput(taskCount);
            double traditionalThroughput = (taskCount * 1000.0) / traditionalTime;
            
            log.info("任务数: {}, 框架吞吐量: {} 任务/秒, 传统吞吐量: {} 任务/秒, 提升: {}%",
                    taskCount, throughputDf.format(frameworkThroughput), throughputDf.format(traditionalThroughput), 
                    throughputDf.format(((frameworkThroughput - traditionalThroughput) / traditionalThroughput) * 100));
        }
    }

    /**
     * 延迟基准测试
     */
    @Test
    public void benchmarkLatency() throws InterruptedException, ExecutionException {
        log.info("=== 延迟基准测试 ===");
        
        int testRounds = 1000;
        List<Long> latencies = new ArrayList<>();
        
        for (int i = 0; i < testRounds; i++) {
            long submitTime = System.nanoTime();
            
            Future<String> future = taskManager.submit("performance-executor", new CallableTask<String>() {
                @Override
                protected String callTask() throws Exception {
                    return "Task completed at " + System.nanoTime();
                }
            });
            
            future.get(); // 等待完成
            long completeTime = System.nanoTime();
            
            latencies.add(completeTime - submitTime);
        }
        
        // 计算延迟统计
        DecimalFormat latencyDf = new DecimalFormat("#.##");
        latencies.sort(Long::compareTo);
        long min = latencies.get(0);
        long max = latencies.get(latencies.size() - 1);
        long median = latencies.get(latencies.size() / 2);
        long p95 = latencies.get((int) (latencies.size() * 0.95));
        long p99 = latencies.get((int) (latencies.size() * 0.99));
        double avg = latencies.stream().mapToLong(Long::longValue).average().orElse(0);
        
        log.info("延迟统计 (微秒):");
        log.info("最小值: {}, 最大值: {}, 平均值: {}", 
                latencyDf.format(min / 1000.0), latencyDf.format(max / 1000.0), latencyDf.format(avg / 1000.0));
        log.info("中位数: {}, P95: {}, P99: {}", 
                latencyDf.format(median / 1000.0), latencyDf.format(p95 / 1000.0), latencyDf.format(p99 / 1000.0));
    }

    /**
     * 并发性能测试
     */
    @Test
    public void benchmarkConcurrency() throws InterruptedException {
        log.info("=== 并发性能测试 ===");
        
        int[] concurrencyLevels = {10, 50, 100, 200};
        int tasksPerThread = 100;
        
        for (int concurrency : concurrencyLevels) {
            log.info("并发级别: {}", concurrency);
            
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch completeLatch = new CountDownLatch(concurrency);
            AtomicLong totalTime = new AtomicLong(0);
            AtomicInteger errorCount = new AtomicInteger(0);
            
            // 创建并发线程
            for (int i = 0; i < concurrency; i++) {
                final int threadId = i;
                new Thread(() -> {
                    try {
                        startLatch.await(); // 等待统一开始
                        long threadStartTime = System.currentTimeMillis();
                        
                        // 每个线程提交指定数量的任务
                        List<Future<?>> futures = new ArrayList<>();
                        for (int j = 0; j < tasksPerThread; j++) {
                            futures.add(taskManager.submit("performance-executor", new CallableTask<Integer>() {
                                @Override
                                protected Integer callTask() throws Exception {
                                    // 模拟计算任务
                                    int result = 0;
                                    for (int k = 0; k < 1000; k++) {
                                        result += k;
                                    }
                                    return result;
                                }
                            }));
                        }
                        
                        // 等待所有任务完成
                        for (Future<?> future : futures) {
                            try {
                                future.get(10, TimeUnit.SECONDS);
                            } catch (Exception e) {
                                errorCount.incrementAndGet();
                            }
                        }
                        
                        long threadEndTime = System.currentTimeMillis();
                        totalTime.addAndGet(threadEndTime - threadStartTime);
                        
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        errorCount.incrementAndGet();
                    } finally {
                        completeLatch.countDown();
                    }
                }).start();
            }
            
            long testStartTime = System.currentTimeMillis();
            startLatch.countDown(); // 开始测试
            completeLatch.await(60, TimeUnit.SECONDS); // 等待完成
            long testEndTime = System.currentTimeMillis();
            
            DecimalFormat concurrencyDf = new DecimalFormat("#.##");
            double avgThreadTime = totalTime.get() / (double) concurrency;
            int totalTasks = concurrency * tasksPerThread;
            double throughput = (totalTasks * 1000.0) / (testEndTime - testStartTime);
            
            log.info("并发级别: {}, 总任务数: {}, 总耗时: {}ms, 平均线程耗时: {}ms, 吞吐量: {} 任务/秒, 错误数: {}",
                    concurrency, totalTasks, testEndTime - testStartTime, concurrencyDf.format(avgThreadTime), concurrencyDf.format(throughput), errorCount.get());
        }
    }

    /**
     * 负载突增测试
     */
    @Test
    public void benchmarkLoadSpike() throws InterruptedException {
        log.info("=== 负载突增测试 ===");
        
        AtomicInteger completedTasks = new AtomicInteger(0);
        AtomicLong totalResponseTime = new AtomicLong(0);
        
        // 阶段1：正常负载
        log.info("阶段1：正常负载 (50 任务/秒)");
        simulateLoad(50, 10, completedTasks, totalResponseTime);
        Thread.sleep(5000);
        
        // 阶段2：负载突增
        log.info("阶段2：负载突增 (500 任务/秒)");
        long spikeStartTime = System.currentTimeMillis();
        simulateLoad(500, 5, completedTasks, totalResponseTime);
        long spikeEndTime = System.currentTimeMillis();
        
        // 阶段3：负载恢复
        log.info("阶段3：负载恢复 (100 任务/秒)");
        simulateLoad(100, 10, completedTasks, totalResponseTime);
        Thread.sleep(10000); // 等待系统稳定
        
        DecimalFormat spikeDf = new DecimalFormat("#.##");
        double avgResponseTime = totalResponseTime.get() / (double) completedTasks.get();
        log.info("负载突增测试结果:");
        log.info("总完成任务: {}, 平均响应时间: {}ms, 突增处理时间: {}ms", 
                completedTasks.get(), spikeDf.format(avgResponseTime), spikeEndTime - spikeStartTime);
    }

    /**
     * 内存使用基准测试
     */
    @Test
    public void benchmarkMemoryUsage() throws InterruptedException {
        log.info("=== 内存使用基准测试 ===");
        
        Runtime runtime = Runtime.getRuntime();
        long initialMemory = runtime.totalMemory() - runtime.freeMemory();
        
        // 执行大量任务
        int taskCount = 5000;
        CountDownLatch latch = new CountDownLatch(taskCount);
        
        for (int i = 0; i < taskCount; i++) {
            taskManager.execute("performance-executor", new RunnableTask() {
                @Override
                protected void runTask() {
                    try {
                        // 模拟内存使用
                        byte[] data = new byte[1024 * 10]; // 10KB
                        Thread.sleep(1);
                        data = null; // 帮助GC
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        
        latch.await();
        
        // 强制GC并等待
        System.gc();
        Thread.sleep(2000);
        
        long finalMemory = runtime.totalMemory() - runtime.freeMemory();
        long memoryIncrease = finalMemory - initialMemory;
        
        log.info("内存使用测试结果:");
        log.info("初始内存: {}MB, 最终内存: {}MB, 增长: {}MB", 
                initialMemory / 1024 / 1024, finalMemory / 1024 / 1024, memoryIncrease / 1024 / 1024);
        log.info("每任务平均内存增长: {}bytes", memoryIncrease / taskCount);
    }

    // ==================== 辅助方法 ====================

    /**
     * 预热JVM
     */
    private void warmUp(int taskCount) throws InterruptedException {
        CountDownLatch warmUpLatch = new CountDownLatch(taskCount);
        for (int i = 0; i < taskCount; i++) {
            taskManager.execute("performance-executor", new RunnableTask() {
                @Override
                protected void runTask() {
                    // 简单计算
                    int result = 0;
                    for (int j = 0; j < 100; j++) {
                        result += j;
                    }
                    warmUpLatch.countDown();
                }
            });
        }
        warmUpLatch.await();
    }

    /**
     * 测量框架吞吐量
     */
    private long measureThroughput(int taskCount, String executorName) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(taskCount);
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < taskCount; i++) {
            taskManager.execute(executorName, new RunnableTask() {
                @Override
                protected void runTask() {
                    // 模拟轻量级计算
                    int result = 0;
                    for (int j = 0; j < 500; j++) {
                        result += j;
                    }
                    latch.countDown();
                }
            });
        }
        
        latch.await();
        return System.currentTimeMillis() - startTime;
    }

    /**
     * 测量传统线程池吞吐量
     */
    private long measureTraditionalThroughput(int taskCount) throws InterruptedException {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                10, 10, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>()
        );
        
        CountDownLatch latch = new CountDownLatch(taskCount);
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < taskCount; i++) {
            executor.execute(() -> {
                // 模拟轻量级计算
                int result = 0;
                for (int j = 0; j < 500; j++) {
                    result += j;
                }
                latch.countDown();
            });
        }
        
        latch.await();
        long endTime = System.currentTimeMillis();
        executor.shutdown();
        
        return endTime - startTime;
    }

    /**
     * 模拟指定速率的负载
     */
    private void simulateLoad(int tasksPerSecond, int durationSeconds, 
                             AtomicInteger completedTasks, AtomicLong totalResponseTime) {
        long intervalNanos = 1_000_000_000L / tasksPerSecond;
        long intervalMillis = intervalNanos / 1_000_000L;  // 转换为毫秒部分
        int remainingNanos = (int) (intervalNanos % 1_000_000L);  // 剩余纳秒部分
        
        long endTime = System.currentTimeMillis() + (durationSeconds * 1000L);
        
        while (System.currentTimeMillis() < endTime) {
            long submitTime = System.currentTimeMillis();
            taskManager.execute("dynamic-executor", new RunnableTask() {
                @Override
                protected void runTask() {
                    try {
                        Thread.sleep(1 + (int)(Math.random() * 9)); // 1-10ms随机任务
                        long completeTime = System.currentTimeMillis();
                        totalResponseTime.addAndGet(completeTime - submitTime);
                        completedTasks.incrementAndGet();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            
            // 控制提交速率 - 正确处理毫秒和纳秒
            try {
                if (intervalMillis > 0) {
                    Thread.sleep(intervalMillis, remainingNanos);
                } else {
                    Thread.sleep(0, remainingNanos);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
} 