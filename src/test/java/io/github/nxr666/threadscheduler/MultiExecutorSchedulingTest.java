package io.github.nxr666.threadscheduler;

import io.github.nxr666.threadscheduler.core.TaskManager;
import io.github.nxr666.threadscheduler.task.RunnableTask;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.text.DecimalFormat;

/**
 * 多执行器调度性能测试 - 框架核心价值验证
 */
@Slf4j
@Tag("slow")
@SpringBootTest
public class MultiExecutorSchedulingTest {

    @Autowired
    private TaskManager taskManager;

    /**
     * 核心测试：多执行器并发调度性能对比
     * 这是最重要的测试，验证智能调度的真正价值
     */
    @Test
    public void testMultiExecutorSchedulingPerformance() throws InterruptedException {
        log.info("=== 多执行器并发调度性能对比测试 ===");
        
        int cpuTasks = 400;       // CPU密集型任务
        int ioTasks = 1000;       // IO密集型任务
        int quickTasks = 2000;    // 快速任务
        
        log.info("测试任务分布 - CPU: {}, IO: {}, Quick: {}, 总计: {}", 
                cpuTasks, ioTasks, quickTasks, cpuTasks + ioTasks + quickTasks);
        
        // 场景1：智能调度框架
        long smartTime = testWithSmartScheduling(cpuTasks, ioTasks, quickTasks);

        // 场景2：传统线程池
        long traditionalTime = testWithFixedThreadPools(cpuTasks, ioTasks, quickTasks);
        // 性能分析
        analyzeResults(smartTime,traditionalTime);
    }

    /**
     * 测试智能调度框架
     */
    private long testWithSmartScheduling(int cpuTasks, int ioTasks, int quickTasks) throws InterruptedException {
        log.info("--- 场景1: 智能调度框架 ---");
        
        CountDownLatch latch = new CountDownLatch(cpuTasks + ioTasks + quickTasks);
        AtomicInteger completed = new AtomicInteger(0);
        
        long startTime = System.currentTimeMillis();
        
        // 并发提交不同类型任务
        submitTasksConcurrently(cpuTasks, ioTasks, quickTasks, latch, completed);
        
        latch.await();
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        
        log.info("智能调度 - 耗时: {}ms, 完成: {}", totalTime, completed.get());
        return totalTime;
    }

    /**
     * 测试传统线程池
     */
    private long testWithFixedThreadPools(int cpuTasks, int ioTasks, int quickTasks) throws InterruptedException {
        log.info("--- 场景2: 传统线程池 ---");
        
        // 调整队列大小和拒绝策略，确保能处理所有任务
        ThreadPoolExecutor cpuPool = new ThreadPoolExecutor(4, 4, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1000000), new ThreadPoolExecutor.CallerRunsPolicy());
        ThreadPoolExecutor ioPool = new ThreadPoolExecutor(20, 20, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1000000), new ThreadPoolExecutor.CallerRunsPolicy());
        ThreadPoolExecutor quickPool = new ThreadPoolExecutor(8, 8, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1000000), new ThreadPoolExecutor.CallerRunsPolicy());
        
        CountDownLatch latch = new CountDownLatch(cpuTasks + ioTasks + quickTasks);
        AtomicInteger completed = new AtomicInteger(0);
        
        long startTime = System.currentTimeMillis();
        
        // 提交任务到固定线程池
        submitToFixedPools(cpuPool, ioPool, quickPool, cpuTasks, ioTasks, quickTasks, latch, completed);
        
        latch.await();
        long endTime = System.currentTimeMillis();
        
        // 关闭线程池
        cpuPool.shutdown();
        ioPool.shutdown();
        quickPool.shutdown();
        
        long totalTime = endTime - startTime;
        log.info("传统线程池 - 耗时: {}ms, 完成: {}", totalTime, completed.get());
        return totalTime;
    }


    /**
     * 性能结果分析
     */
    private void analyzeResults(long smartTime, long traditionalTime) {
        log.info("=== 性能对比分析 ===");
        
        DecimalFormat df = new DecimalFormat("#.##");
        double smartVsTraditional = ((double)(traditionalTime - smartTime) / traditionalTime) * 100;
        
        log.info("执行时间对比:");
        log.info("  智能调度: {}ms", smartTime);
        log.info("  传统线程池: {}ms", traditionalTime);
        
        log.info("性能提升:");
        log.info("  智能调度 vs 传统线程池: {}%", df.format(smartVsTraditional));

        if (smartVsTraditional > 0) {
            log.info("✅ 智能调度优于传统线程池 {}%", df.format(smartVsTraditional));
        }
    }

    /**
     * 负载不均衡测试
     */
    @Test
    public void testUnbalancedLoad() throws InterruptedException {
        log.info("=== 负载不均衡场景测试 ===");
        
        CountDownLatch latch = new CountDownLatch(3000);
        AtomicInteger completed = new AtomicInteger(0);
        
        long startTime = System.currentTimeMillis();
        
        // 大量IO任务 + 少量CPU任务，测试线程调度能力
        for (int i = 0; i < 2500; i++) {
            taskManager.execute("io-executor", new RunnableTask() {
                @Override
                protected void runTask() {
                    try {
                        Thread.sleep(10 + (int)(Math.random() * 20));
                        completed.incrementAndGet();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        
        for (int i = 0; i < 500; i++) {
            taskManager.execute("cpu-executor", new RunnableTask() {
                @Override
                protected void runTask() {
                    long result = 0;
                    for (int j = 0; j < 10000; j++) {
                        result += Math.sqrt(j);
                    }
                    completed.incrementAndGet();
                    latch.countDown();
                }
            });
        }
        
        latch.await();
        long endTime = System.currentTimeMillis();
        
        log.info("负载不均衡测试 - 耗时: {}ms, 完成: {}", 
                endTime - startTime, completed.get());
    }

    // ==================== 辅助方法 ====================

    private void submitTasksConcurrently(int cpuTasks, int ioTasks, int quickTasks, 
                                        CountDownLatch latch, AtomicInteger completed) {
        // 并发提交CPU任务
        CompletableFuture.runAsync(() -> {
            for (int i = 0; i < cpuTasks; i++) {
                taskManager.execute("cpu-executor", new RunnableTask() {
                    @Override
                    protected void runTask() {
                        performCpuTask();
                        completed.incrementAndGet();
                        latch.countDown();
                    }
                });
            }
        });

        // 并发提交IO任务
        CompletableFuture.runAsync(() -> {
            for (int i = 0; i < ioTasks; i++) {
                taskManager.execute("io-executor", new RunnableTask() {
                    @Override
                    protected void runTask() {
                        try {
                            performIoTask();
                            completed.incrementAndGet();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            latch.countDown();
                        }
                    }
                });
            }
        });

        // 并发提交快速任务
        CompletableFuture.runAsync(() -> {
            for (int i = 0; i < quickTasks; i++) {
                taskManager.execute("quick-executor", new RunnableTask() {
                    @Override
                    protected void runTask() {
                        performQuickTask();
                        completed.incrementAndGet();
                        latch.countDown();
                    }
                });
            }
        });
    }

    private void submitToFixedPools(ThreadPoolExecutor cpuPool, ThreadPoolExecutor ioPool, 
                                   ThreadPoolExecutor quickPool, int cpuTasks, int ioTasks, 
                                   int quickTasks, CountDownLatch latch, AtomicInteger completed) {
        // 提交CPU任务
        for (int i = 0; i < cpuTasks; i++) {
            cpuPool.execute(() -> {
                performCpuTask();
                completed.incrementAndGet();
                latch.countDown();
            });
        }

        // 提交IO任务
        for (int i = 0; i < ioTasks; i++) {
            ioPool.execute(() -> {
                try {
                    performIoTask();
                    completed.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    latch.countDown();
                }
            });
        }

        // 提交快速任务
        for (int i = 0; i < quickTasks; i++) {
            quickPool.execute(() -> {
                performQuickTask();
                completed.incrementAndGet();
                latch.countDown();
            });
        }
    }

    private void submitToTraditionalPools(ExecutorService cpuPool, ExecutorService ioPool, 
                                         ExecutorService quickPool, int cpuTasks, int ioTasks, 
                                         int quickTasks, CountDownLatch latch, AtomicInteger completed) {
        // 提交CPU任务
        for (int i = 0; i < cpuTasks; i++) {
            cpuPool.execute(() -> {
                performCpuTask();
                completed.incrementAndGet();
                latch.countDown();
            });
        }

        // 提交IO任务
        for (int i = 0; i < ioTasks; i++) {
            ioPool.execute(() -> {
                try {
                    performIoTask();
                    completed.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    latch.countDown();
                }
            });
        }

        // 提交快速任务
        for (int i = 0; i < quickTasks; i++) {
            quickPool.execute(() -> {
                performQuickTask();
                completed.incrementAndGet();
                latch.countDown();
            });
        }
    }

    private void performCpuTask() {
        System.out.println("run cpu task");
        long result = 0;
        for (int j = 0; j < 50000; j++) {
            result += Math.sqrt(j);
        }
    }

    private void performIoTask() throws InterruptedException {
        Thread.sleep(1000 + (int)(Math.random() * 1000)); //1-2s
        System.out.println("run io task");
    }

    private void performQuickTask() {

        try {
            Thread.sleep((int)(Math.random() * 1000)); //1s以内
            System.out.println("run quick task");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
} 