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
@SpringBootTest(classes = io.github.nxr666.threadscheduler.ThreadSchedulerApplication.class)
public class StressTest {

    @Autowired
    private TaskManager taskManager;

    /**
     * 极限并发压力测试
     */
    @Test
    public void extremeConcurrencyStressTest() throws InterruptedException {
        log.info("=== 极限并发压力测试 ===");
        
        int threadCount = 500;  // 500个并发线程
        int tasksPerThread = 1000;  // 每线程1000个任务
        int totalTasks = threadCount * tasksPerThread;
        
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        AtomicLong totalTime = new AtomicLong(0);
        
        log.info("启动 {} 个并发线程，总任务数: {}", threadCount, totalTasks);
        
        // 创建压力线程
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            new Thread(() -> {
                try {
                    startLatch.await();
                    long threadStart = System.currentTimeMillis();
                    
                    List<Future<?>> futures = new ArrayList<>();
                    for (int j = 0; j < tasksPerThread; j++) {
                        try {
                            Future<?> future = taskManager.submit("performance-executor", new CallableTask<String>() {
                                @Override
                                protected String callTask() throws Exception {
                                    // 模拟复杂计算
                                    long result = 0;
                                    for (int k = 0; k < 10000; k++) {
                                        result += Math.sqrt(k * threadId);
                                    }
                                    return "Thread-" + threadId + "-Result-" + result;
                                }
                            });
                            futures.add(future);
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                        }
                    }
                    
                    // 等待任务完成
                    for (Future<?> future : futures) {
                        try {
                            future.get(30, TimeUnit.SECONDS);
                            successCount.incrementAndGet();
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                        }
                    }
                    
                    long threadEnd = System.currentTimeMillis();
                    totalTime.addAndGet(threadEnd - threadStart);
                    
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    errorCount.addAndGet(tasksPerThread);
                } finally {
                    completeLatch.countDown();
                }
            }).start();
        }
        
        long testStart = System.currentTimeMillis();
        startLatch.countDown();
        boolean completed = completeLatch.await(300, TimeUnit.SECONDS); // 5分钟超时
        long testEnd = System.currentTimeMillis();
        
        DecimalFormat df = new DecimalFormat("#.##");
        double successRate = (successCount.get() * 100.0) / totalTasks;
        double avgThreadTime = totalTime.get() / (double) threadCount;
        double throughput = (successCount.get() * 1000.0) / (testEnd - testStart);
        
        log.info("=== 极限并发压力测试结果 ===");
        log.info("测试完成: {}, 总耗时: {}ms", completed, testEnd - testStart);
        log.info("成功任务: {}, 失败任务: {}, 成功率: {}%", 
                successCount.get(), errorCount.get(), df.format(successRate));
        log.info("平均线程耗时: {}ms, 系统吞吐量: {} 任务/秒", df.format(avgThreadTime), df.format(throughput));
    }

    /**
     * 长时间运行稳定性测试
     */
    @Test
    public void longRunningStabilityTest() throws InterruptedException {
        log.info("=== 长时间运行稳定性测试 ===");
        
        int durationMinutes = 10; // 运行10分钟
        int tasksPerSecond = 100;  // 每秒100个任务
        
        AtomicInteger totalSubmitted = new AtomicInteger(0);
        AtomicInteger totalCompleted = new AtomicInteger(0);
        AtomicInteger totalErrors = new AtomicInteger(0);
        AtomicLong totalResponseTime = new AtomicLong(0);
        
        // 启动监控线程
        ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();
        DecimalFormat monitorDf = new DecimalFormat("#.##");
        monitor.scheduleAtFixedRate(() -> {
            Runtime runtime = Runtime.getRuntime();
            long usedMemory = runtime.totalMemory() - runtime.freeMemory();
            double memoryUsage = (usedMemory * 100.0) / runtime.maxMemory();
            
            log.info("运行状态 - 提交: {}, 完成: {}, 错误: {}, 内存使用: {}%", 
                    totalSubmitted.get(), totalCompleted.get(), totalErrors.get(), monitorDf.format(memoryUsage));
        }, 0, 30, TimeUnit.SECONDS);
        
        long endTime = System.currentTimeMillis() + (durationMinutes * 60 * 1000L);
        long intervalMs = 1000 / tasksPerSecond;
        
        while (System.currentTimeMillis() < endTime) {
            long submitTime = System.currentTimeMillis();
            
            try {
                taskManager.execute("performance-executor", new RunnableTask() {
                    @Override
                    protected void runTask() {
                        try {
                            // 模拟不同类型的任务
                            int taskType = (int) (Math.random() * 3);
                            switch (taskType) {
                                case 0: // CPU密集型
                                    performCpuTask();
                                    break;
                                case 1: // IO密集型
                                    performIoTask();
                                    break;
                                case 2: // 混合型
                                    performMixedTask();
                                    break;
                            }
                            
                            long completeTime = System.currentTimeMillis();
                            totalResponseTime.addAndGet(completeTime - submitTime);
                            totalCompleted.incrementAndGet();
                            
                        } catch (Exception e) {
                            totalErrors.incrementAndGet();
                        }
                    }
                });
                
                totalSubmitted.incrementAndGet();
                
            } catch (Exception e) {
                totalErrors.incrementAndGet();
            }
            
            // 控制提交速率
            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        
        // 等待所有任务完成
        Thread.sleep(30000);
        monitor.shutdown();
        
        DecimalFormat resultDf = new DecimalFormat("#.##");
        double completionRate = (totalCompleted.get() * 100.0) / totalSubmitted.get();
        double avgResponseTime = totalResponseTime.get() / (double) totalCompleted.get();
        
        log.info("=== 长时间运行稳定性测试结果 ===");
        log.info("运行时长: {} 分钟", durationMinutes);
        log.info("提交任务: {}, 完成任务: {}, 错误任务: {}", 
                totalSubmitted.get(), totalCompleted.get(), totalErrors.get());
        log.info("完成率: {}%, 平均响应时间: {}ms", resultDf.format(completionRate), resultDf.format(avgResponseTime));
    }

    /**
     * 内存泄漏检测测试
     */
    @Test
    public void memoryLeakDetectionTest() throws InterruptedException {
        log.info("=== 内存泄漏检测测试 ===");
        
        Runtime runtime = Runtime.getRuntime();
        long initialMemory = runtime.totalMemory() - runtime.freeMemory();
        
        List<Long> memorySnapshots = new ArrayList<>();
        memorySnapshots.add(initialMemory);
        
        // 执行多轮任务
        int rounds = 10;
        int tasksPerRound = 1000;
        
        for (int round = 0; round < rounds; round++) {
            log.info("执行第 {} 轮任务", round + 1);
            
            CountDownLatch roundLatch = new CountDownLatch(tasksPerRound);
            
            for (int i = 0; i < tasksPerRound; i++) {
                taskManager.execute("resource-executor", new RunnableTask() {
                    @Override
                    protected void runTask() {
                        try {
                            // 创建临时对象
                            List<byte[]> tempData = new ArrayList<>();
                            for (int j = 0; j < 100; j++) {
                                tempData.add(new byte[1024]); // 1KB
                            }
                            
                            // 模拟处理
                            Thread.sleep(1);
                            
                            // 清理
                            tempData.clear();
                            
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            roundLatch.countDown();
                        }
                    }
                });
            }
            
            roundLatch.await();
            
            // 强制GC并记录内存
            System.gc();
            Thread.sleep(1000);
            
            long currentMemory = runtime.totalMemory() - runtime.freeMemory();
            memorySnapshots.add(currentMemory);
            
            log.info("第 {} 轮完成，当前内存: {}MB", round + 1, currentMemory / 1024 / 1024);
        }
        
        // 分析内存趋势
        boolean memoryLeak = false;
        long maxIncrease = 0;
        for (int i = 1; i < memorySnapshots.size(); i++) {
            long increase = memorySnapshots.get(i) - memorySnapshots.get(0);
            if (increase > maxIncrease) {
                maxIncrease = increase;
            }
            
            // 如果内存增长超过初始内存的50%，可能存在泄漏
            if (increase > initialMemory * 0.5) {
                memoryLeak = true;
            }
        }
        
        log.info("=== 内存泄漏检测结果 ===");
        log.info("初始内存: {}MB, 最大增长: {}MB", initialMemory / 1024 / 1024, maxIncrease / 1024 / 1024);
        log.info("可能存在内存泄漏: {}", memoryLeak);
    }

    /**
     * 异常恢复能力测试
     */
    @Test
    public void exceptionRecoveryTest() throws InterruptedException {
        log.info("=== 异常恢复能力测试 ===");
        
        AtomicInteger normalTasks = new AtomicInteger(0);
        AtomicInteger exceptionTasks = new AtomicInteger(0);
        AtomicInteger recoveredTasks = new AtomicInteger(0);
        
        int totalTasks = 1000;
        int exceptionRate = 20; // 20%的任务会抛异常
        
        CountDownLatch latch = new CountDownLatch(totalTasks);
        
        for (int i = 0; i < totalTasks; i++) {
            final int taskId = i;
            final boolean shouldThrow = (Math.random() * 100) < exceptionRate;
            
            taskManager.execute("performance-executor", new RunnableTask() {
                @Override
                protected void runTask() {
                    try {
                        if (shouldThrow) {
                            exceptionTasks.incrementAndGet();
                            throw new RuntimeException("模拟任务异常 - Task " + taskId);
                        } else {
                            // 正常任务
                            Thread.sleep(1 + (int)(Math.random() * 9));
                            normalTasks.incrementAndGet();
                        }
                    } catch (Exception e) {
                        // 异常处理后继续执行
                        try {
                            Thread.sleep(1);
                            recoveredTasks.incrementAndGet();
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        
        latch.await();
        
        DecimalFormat recoveryDf = new DecimalFormat("#.##");
        double recoveryRate = (recoveredTasks.get() * 100.0) / exceptionTasks.get();
        
        log.info("=== 异常恢复能力测试结果 ===");
        log.info("总任务: {}, 正常任务: {}, 异常任务: {}, 恢复任务: {}", 
                totalTasks, normalTasks.get(), exceptionTasks.get(), recoveredTasks.get());
        log.info("异常恢复率: {}%", recoveryDf.format(recoveryRate));
    }

    // ==================== 辅助方法 ====================

    private void performCpuTask() {
        long result = 0;
        for (int i = 0; i < 50000; i++) {
            result += Math.sqrt(i);
        }
    }

    private void performIoTask() throws InterruptedException {
        Thread.sleep(5 + (int)(Math.random() * 15)); // 5-20ms
    }

    private void performMixedTask() throws InterruptedException {
        // CPU计算
        long result = 0;
        for (int i = 0; i < 10000; i++) {
            result += i * i;
        }
        
        // IO等待
        Thread.sleep(1 + (int)(Math.random() * 4)); // 1-5ms
    }
} 