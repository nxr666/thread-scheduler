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
public class PerformanceTest {

    @Autowired
    private TaskManager taskManager;

    /**
     * 测试场景1：高并发短任务处理效率对比
     */
    @Test
    public void testHighConcurrencyShortTasks() throws InterruptedException {
        int taskCount = 10000;
        int threadCount = 50;
        
        log.info("=== 高并发短任务处理效率测试 ===");
        log.info("任务数量: {}, 线程数: {}", taskCount, threadCount);
        
        // 1. 使用传统线程池
        long traditionalTime = testWithTraditionalThreadPool(taskCount, threadCount);
        
        // 2. 使用调度框架
        long frameworkTime = testWithSchedulerFramework(taskCount);
        
        // 3. 性能对比
        DecimalFormat df = new DecimalFormat("#.##");
        double improvement = ((double) (traditionalTime - frameworkTime) / traditionalTime) * 100;
        log.info("=== 性能对比结果 ===");
        log.info("传统线程池耗时: {}ms", traditionalTime);
        log.info("调度框架耗时: {}ms", frameworkTime);
        log.info("性能提升: {}%", df.format(improvement));
    }

    /**
     * 测试场景2：混合负载任务处理效率
     */
    @Test
    public void testMixedWorkloadTasks() throws InterruptedException {
        log.info("=== 混合负载任务处理效率测试 ===");
        
        // 模拟不同类型的任务
        int cpuIntensiveCount = 100;    // CPU密集型任务
        int ioIntensiveCount = 500;     // IO密集型任务
        int quickTaskCount = 1000;      // 快速任务
        
        long startTime = System.currentTimeMillis();
        
        // 使用不同的执行器处理不同类型的任务
        List<Future<?>> futures = new ArrayList<>();
        
        // CPU密集型任务 - 使用cpu-executor
        for (int i = 0; i < cpuIntensiveCount; i++) {
            final int taskId = i;
            futures.add(taskManager.submit("cpu-executor", new CallableTask<String>() {
                @Override
                protected String callTask() throws Exception {
                    return performCpuIntensiveTask(taskId);
                }
            }));
        }
        
        // IO密集型任务 - 使用io-executor
        for (int i = 0; i < ioIntensiveCount; i++) {
            final int taskId = i;
            futures.add(taskManager.submit("io-executor", new CallableTask<String>() {
                @Override
                protected String callTask() throws Exception {
                    return performIoIntensiveTask(taskId);
                }
            }));
        }
        
        // 快速任务 - 使用quick-executor
        for (int i = 0; i < quickTaskCount; i++) {
            final int taskId = i;
            taskManager.execute("quick-executor", new RunnableTask() {
                @Override
                protected void runTask() {
                    performQuickTask(taskId);
                }
            });
        }
        
        // 等待所有任务完成
        for (Future<?> future : futures) {
            try {
                future.get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.error("任务执行失败", e);
            }
        }
        
        long endTime = System.currentTimeMillis();
        log.info("混合负载任务处理完成，总耗时: {}ms", endTime - startTime);
        log.info("CPU密集型任务: {}, IO密集型任务: {}, 快速任务: {}", 
                cpuIntensiveCount, ioIntensiveCount, quickTaskCount);
    }

    /**
     * 测试场景3：动态线程调整效果
     */
    @Test
    public void testDynamicThreadAdjustment() throws InterruptedException {
        log.info("=== 动态线程调整效果测试 ===");
        
        AtomicInteger completedTasks = new AtomicInteger(0);
        AtomicLong totalResponseTime = new AtomicLong(0);
        
        // 阶段1：低负载
        log.info("阶段1：低负载测试");
        submitTaskBatch("dynamic-executor", 50, completedTasks, totalResponseTime);
        Thread.sleep(5000); // 等待5秒观察线程调整
        
        // 阶段2：中等负载
        log.info("阶段2：中等负载测试");
        submitTaskBatch("dynamic-executor", 200, completedTasks, totalResponseTime);
        Thread.sleep(5000);
        
        // 阶段3：高负载
        log.info("阶段3：高负载测试");
        submitTaskBatch("dynamic-executor", 500, completedTasks, totalResponseTime);
        Thread.sleep(10000); // 等待更长时间处理高负载
        
        // 阶段4：负载下降
        log.info("阶段4：负载下降测试");
        submitTaskBatch("dynamic-executor", 100, completedTasks, totalResponseTime);
        Thread.sleep(15000); // 等待空闲线程回收
        
        DecimalFormat avgDf = new DecimalFormat("#.##");
        double avgResponseTime = totalResponseTime.get() / (double) completedTasks.get();
        log.info("=== 动态调整测试结果 ===");
        log.info("总完成任务数: {}", completedTasks.get());
        log.info("平均响应时间: {}ms", avgDf.format(avgResponseTime));
    }

    /**
     * 测试场景4：资源利用率监控
     */
    @Test
    public void testResourceUtilization() throws InterruptedException {
        log.info("=== 资源利用率监控测试 ===");
        
        // 启动监控线程
        ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();
        DecimalFormat memDf = new DecimalFormat("#.##");
        monitor.scheduleAtFixedRate(() -> {
            Runtime runtime = Runtime.getRuntime();
            long totalMemory = runtime.totalMemory();
            long freeMemory = runtime.freeMemory();
            long usedMemory = totalMemory - freeMemory;
            double memoryUsage = (usedMemory * 100.0) / totalMemory;
            
            log.info("内存使用率: {}%, 已用内存: {}MB, 总内存: {}MB", 
                    memDf.format(memoryUsage), usedMemory / 1024 / 1024, totalMemory / 1024 / 1024);
        }, 0, 2, TimeUnit.SECONDS);
        
        // 执行大量任务
        for (int batch = 0; batch < 5; batch++) {
            log.info("执行第 {} 批任务", batch + 1);
            for (int i = 0; i < 200; i++) {
                final int taskId = i;
                taskManager.execute("resource-executor", new RunnableTask() {
                    @Override
                    protected void runTask() {
                        performMemoryIntensiveTask(taskId);
                    }
                });
            }
            Thread.sleep(3000);
        }
        
        Thread.sleep(10000); // 等待任务完成和资源回收
        monitor.shutdown();
    }

    /**
     * 使用传统线程池测试
     */
    private long testWithTraditionalThreadPool(int taskCount, int threadCount) throws InterruptedException {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                threadCount, threadCount, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>()
        );
        
        long startTime = System.currentTimeMillis();
        CountDownLatch latch = new CountDownLatch(taskCount);
        
        for (int i = 0; i < taskCount; i++) {
            final int taskId = i;
            executor.execute(() -> {
                try {
                    performShortTask(taskId);
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await();
        long endTime = System.currentTimeMillis();
        executor.shutdown();
        
        return endTime - startTime;
    }

    /**
     * 使用调度框架测试
     */
    private long testWithSchedulerFramework(int taskCount) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        CountDownLatch latch = new CountDownLatch(taskCount);
        
        for (int i = 0; i < taskCount; i++) {
            final int taskId = i;
            taskManager.execute("performance-executor", new RunnableTask() {
                @Override
                protected void runTask() {
                    try {
                        performShortTask(taskId);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        
        latch.await();
        long endTime = System.currentTimeMillis();
        
        return endTime - startTime;
    }

    /**
     * 提交任务批次
     */
    private void submitTaskBatch(String executorName, int count, 
                                AtomicInteger completedTasks, AtomicLong totalResponseTime) {
        for (int i = 0; i < count; i++) {
            final int taskId = i;
            final long submitTime = System.currentTimeMillis();
            taskManager.execute(executorName, new RunnableTask() {
                @Override
                protected void runTask() {
                    performVariableTask(taskId);
                    long completeTime = System.currentTimeMillis();
                    totalResponseTime.addAndGet(completeTime - submitTime);
                    completedTasks.incrementAndGet();
                }
            });
        }
    }

    // ==================== 模拟任务方法 ====================

    /**
     * 短任务 - 模拟简单计算
     */
    private void performShortTask(int taskId) {
        try {
            // 模拟1-5ms的计算
            Thread.sleep(1 + (int)(Math.random() * 4));
            int result = 0;
            for (int i = 0; i < 1000; i++) {
                result += i * taskId;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * CPU密集型任务
     */
    private String performCpuIntensiveTask(int taskId) {
        // 模拟CPU密集计算
        long result = 0;
        for (int i = 0; i < 100000; i++) {
            result += Math.sqrt(i * taskId);
        }
        return "CPU Task " + taskId + " result: " + result;
    }

    /**
     * IO密集型任务
     */
    private String performIoIntensiveTask(int taskId) throws InterruptedException {
        // 模拟IO等待
        Thread.sleep(10 + (int)(Math.random() * 20)); // 10-30ms
        return "IO Task " + taskId + " completed";
    }

    /**
     * 快速任务
     */
    private void performQuickTask(int taskId) {
        // 模拟非常快的任务
        int result = taskId * 2 + 1;
    }

    /**
     * 可变时长任务
     */
    private void performVariableTask(int taskId) {
        try {
            // 随机1-100ms的任务
            Thread.sleep(1 + (int)(Math.random() * 99));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 内存密集型任务
     */
    private void performMemoryIntensiveTask(int taskId) {
        // 模拟内存使用
        List<byte[]> memoryList = new ArrayList<>();
        try {
            for (int i = 0; i < 10; i++) {
                memoryList.add(new byte[1024 * 100]); // 100KB
                Thread.sleep(1);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            memoryList.clear(); // 释放内存
        }
    }
} 