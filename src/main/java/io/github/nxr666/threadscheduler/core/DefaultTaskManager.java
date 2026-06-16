package io.github.nxr666.threadscheduler.core;

import io.github.nxr666.threadscheduler.config.TaskManagerConfig;
import io.github.nxr666.threadscheduler.task.CallableTask;
import io.github.nxr666.threadscheduler.task.RunnableTask;
import io.github.nxr666.threadscheduler.exception.ExecutorClosedException;
import io.github.nxr666.threadscheduler.exception.TaskManagerException;
import io.github.nxr666.threadscheduler.executor.GenericTaskExecutor;
import io.github.nxr666.threadscheduler.executor.TaskExecutor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.text.DecimalFormat;

@Slf4j
public class DefaultTaskManager implements TaskManager, InitializingBean {

    private TaskManagerConfig properties;

    private final Map<String, TaskExecutor> executors = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private final AtomicInteger usedThreads = new AtomicInteger(0);

    private int totalThreads;

    // 汇总统计信息
    private long totalTasks = 0;
    private long totalCompleted = 0;
    private long totalRejected = 0;
    private long totalDiscarded = 0;
    private long totalCallerRuns = 0;

    @Override
    public void afterPropertiesSet() {
        if(properties.isEnabled()) {
            this.totalThreads = properties.getTotalThreads();
            // 定时任务只调度 schedule()
            scheduler.scheduleAtFixedRate(this::schedule, 5, properties.getScheduleRate(), TimeUnit.SECONDS);
            log.info("GenericTaskManager initialized");
        }
    }

    /**
     * 定时调度方法，根据配置决定是否调整线程和/或输出状态
     */
    public void schedule() {
        if (properties.isAdjustThreadsOff()) {
            // 只输出状态日志
            logExecutorsStatus();
        } else {
            // 先调整线程，再输出状态日志
            adjustThreads();
            logExecutorsStatus();
        }
    }

    @Override
    public void execute(String executorName, RunnableTask task) {
        TaskExecutor executor = getOrCreateExecutor(executorName);
        try {
            executor.execute(task);
        } catch (ExecutorClosedException e) {
            // 执行器已关闭，重试一次
            log.warn("执行器 [{}] 已关闭，重试获取和执行任务", executorName);
            try {
                try {
                    // 间隔一秒后重试，确保执行器已被完全关闭
                    Thread.sleep(1000);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
                TaskExecutor retryExecutor = getOrCreateExecutor(executorName);
                retryExecutor.execute(task);
            } catch (Exception retryException) {
                // 重试仍然失败，记录错误并重新抛出
                log.error("执行器 [{}] 重试执行任务仍然失败", executorName);
                throw retryException;
            }
        } catch (RejectedExecutionException e) {
            // 其他原因的拒绝（如队列满），不重试，直接抛出让调用方处理
            log.warn("执行器 [{}] 拒绝执行任务，可能是队列满或其他拒绝策略触发: {}", executorName, e.getMessage());
            throw e;
        }
    }

    @Override
    public <T> Future<T> submit(String executorName, CallableTask<T> task) {
        TaskExecutor executor = getOrCreateExecutor(executorName);
        try {
            return executor.submit(task);
        } catch (ExecutorClosedException e) {
            // 执行器已关闭，重试一次
            log.warn("执行器 [{}] 已关闭，重试获取和提交任务", executorName);
            try {
                try {
                    // 间隔一秒后重试，确保执行器已被完全关闭
                    Thread.sleep(1000);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
                TaskExecutor retryExecutor = getOrCreateExecutor(executorName);
                return retryExecutor.submit(task);
            } catch (Exception retryException) {
                // 重试仍然失败，记录错误并重新抛出
                log.error("执行器 [{}] 重试提交任务仍然失败", executorName,retryException);
                throw retryException;
            }
        } catch (RejectedExecutionException e) {
            // 其他原因的拒绝（如队列满），不重试，直接抛出让调用方处理
            log.warn("执行器 [{}] 拒绝提交任务，可能是队列满或其他拒绝策略触发: {}", executorName, e.getMessage());
            throw e;
        }
    }

    @Override
    public void shutdown(String executorName) {
        TaskExecutor executor = executors.get(executorName);
        if (executor != null) {
            synchronized (executor) {
                // 再次检查，确保执行器仍在Map中且未被其他线程关闭
                if (executors.get(executorName) == executor && !executor.isShutdown()) {
                    // 先关闭执行器，阻止新任务提交
                    executor.shutdown();
                    // 再从Map中移除
                    executors.remove(executorName);
                    // 回收线程资源
                    usedThreads.addAndGet(-executor.getCoreSize());
                    log.info("执行器 [{}] 已关闭并从管理器中移除", executorName);
                }
            }
        }
    }

    private synchronized TaskExecutor getOrCreateExecutor(String name) {
        // 1. 先查找已有执行器
        TaskExecutor existing = executors.get(name);
        if (existing != null) {
            synchronized (existing) {
                // 再次从Map中获取，确保执行器仍然有效且未被移除
                TaskExecutor current = executors.get(name);
                if (current == existing && !existing.isShutdown()) {
                    return existing;
                }
            }
        }

        // 2. 查找配置
        Optional<TaskManagerConfig.ExecutorConfig> configOpt = properties.getExecutors().stream()
                .filter(e -> e.getName().equals(name)).findFirst();
        if (!configOpt.isPresent()) {
            throw new TaskManagerException("Executor not configured: " + name);
        }
        TaskManagerConfig.ExecutorConfig config = configOpt.get();
        int coreSize = config.getCoreSize() != null ? config.getCoreSize() : Runtime.getRuntime().availableProcessors();
        int queueSize = config.getQueueSize() != null ? config.getQueueSize() : 100;
        String reject = config.getRejectStrategy() != null ? config.getRejectStrategy() : "CallerRunsPolicy";

        // 3. 判断当前线程资源是否足够
        int availableThreads = totalThreads - usedThreads.get();
        if (availableThreads < coreSize) {
            log.warn("线程资源不足，尝试回收空闲资源，当前可用:{}，需:{}", availableThreads, coreSize);
            // 只回收当前核心线程数大于配置核心线程数的执行器
            List<TaskExecutor> canRecycle = new ArrayList<>();
            for (TaskExecutor e : executors.values()) {
                // 找到初始配置
                Optional<TaskManagerConfig.ExecutorConfig> confOpt = properties.getExecutors().stream()
                        .filter(c -> c.getName().equals(e.getName())).findFirst();
                int configCore = confOpt.map(TaskManagerConfig.ExecutorConfig::getCoreSize).orElse(Runtime.getRuntime().availableProcessors());
                if (e.getCoreSize() > configCore) {
                    canRecycle.add(e);
                }
            }
            // 排序：空闲线程数最多优先，空闲线程数相同则任务队列数量最小优先
            canRecycle.sort((a, b) -> {
                int idleA = a.getCoreSize() - a.getActiveCount();
                int idleB = b.getCoreSize() - b.getActiveCount();
                if (idleB != idleA) {
                    return Integer.compare(idleB, idleA); // 空闲线程多的优先
                } else {
                    return Integer.compare(a.getQueueSizeUsed(), b.getQueueSizeUsed()); // 队列小的优先
                }
            });
            // 循环回收
            for (TaskExecutor e : canRecycle) {
                Optional<TaskManagerConfig.ExecutorConfig> confOpt = properties.getExecutors().stream()
                        .filter(c -> c.getName().equals(e.getName())).findFirst();
                int configCore = confOpt.map(TaskManagerConfig.ExecutorConfig::getCoreSize).orElse(Runtime.getRuntime().availableProcessors());
                int canReduce = e.getCoreSize() - configCore;
                int reduce = Math.min(canReduce, coreSize - availableThreads);
                if (reduce > 0) {
                    e.setCoreSize(e.getCoreSize() - reduce);
                    usedThreads.addAndGet(-reduce);
                    availableThreads += reduce;
                    log.info("优先回收最空闲的执行器 [{}] 的 {} 个核心线程，当前核心线程数:{}", e.getName(), reduce, e.getCoreSize());
                    if (availableThreads >= coreSize) {break;}
                }
            }
            // 再次判断是否足够
            if (availableThreads < coreSize) {
                throw new TaskManagerException("线程资源不足，无法创建执行器: " + name);
            }
        }

        // 5. 创建执行器，扣减可用线程资源
        usedThreads.addAndGet(coreSize);
        GenericTaskExecutor executor = new GenericTaskExecutor(name, coreSize, queueSize, reject);
        executor.create();
        executors.put(name, executor);
        log.info("创建执行器 [{}]，核心线程数:{}，队列大小:{}，拒绝策略:{}，剩余可用线程:{}", name, coreSize, queueSize, reject, totalThreads - usedThreads.get());
        return executor;
    }

    private void adjustThreads() {
        if (properties.isAdjustThreadsOff()) return;
        long now = System.currentTimeMillis();
        int idleTimeout = properties.getTaskExecutorIdleTime() * 1000;

        // 1. 检查是否有超时空闲的执行器
        List<TaskExecutor> toShutdown = new ArrayList<>();
        for (Map.Entry<String, TaskExecutor> entry : executors.entrySet()) {
            TaskExecutor e = entry.getValue();
            // 判断空闲：无活跃线程且队列为空，且空闲时间超时
            if (e.getActiveCount() == 0 && e.getQueueSizeUsed() == 0) {
                // 获取最后一次执行时间（包括任务提交和拒绝处理），判断是否超时
                long lastExecuteTime = e.getLastExecuteTime();
                long idleTime = now - lastExecuteTime;
                if (idleTime > idleTimeout) {
                    toShutdown.add(e);
                    log.info("执行器 [{}] 空闲超时，空闲时间: {}ms，超时配置: {}ms", 
                            e.getName(), idleTime, idleTimeout);
                }
            }
        }
        for (TaskExecutor e : toShutdown) {
            // 关闭这个执行器前，先检查是否有任务正在提交
            synchronized (e) {
                // 再次判断是否空闲且未被关闭，避免重复关闭
                if (e.getActiveCount() == 0 && e.getQueueSizeUsed() == 0 && !e.isShutdown()) {
                    shutdown(e.getName());
                    log.info("关闭超时空闲执行器 [{}] 并回收线程", e.getName());
                    totalTasks +=  e.getTotalTaskCount();
                    totalCompleted += e.getCompletedTaskCount();
                    totalRejected += e.getRejectedTaskCount();
                    totalDiscarded += e.getDiscardedTaskCount();
                    totalCallerRuns += e.getCallerRunsTaskCount();
                }
            }
        }

        // 2. 没有超时空闲执行器，检查是否有可用剩余线程
        int availableThreads = totalThreads - usedThreads.get();
        if (availableThreads <= 0) {
            log.info("无可用剩余线程，当前可用线程数: {}", availableThreads);
            return;
        }

        // 3. 有可用剩余线程，检查是否有任务积压的执行器
        List<TaskExecutor> busyExecutors = new ArrayList<>();
        int totalQueue = 0;
        for (TaskExecutor e : executors.values()) {
            if (e.getQueueSizeUsed() > 0) {
                busyExecutors.add(e);
                totalQueue += e.getQueueSizeUsed();
            }
        }
        if (busyExecutors.isEmpty()) {
            log.info("无任务积压的执行器");
            return;
        }

        // 4. 有任务繁忙的执行器，把可用线程的一半按队列任务数百分比分配
        int threadsToDistribute = availableThreads / 2;
        // 至少分配1个
        if (threadsToDistribute == 0) {
            threadsToDistribute = 1;
        }
        for (TaskExecutor e : busyExecutors) {
            int queue = e.getQueueSizeUsed();
            int add = (int) Math.round((queue * 1.0 / totalQueue) * threadsToDistribute);
            if (add > 0) {
                e.setCoreSize(e.getCoreSize() + add);
                usedThreads.addAndGet(add);
                log.info("为积压执行器 [{}] 补充分配 {} 个线程，当前核心线程数: {}", e.getName(), add, e.getCoreSize());
            }
        }
        log.info("已按队列任务数百分比分配线程，总分配: {} 个", threadsToDistribute);
    }

    private void logExecutorsStatus() {
        log.info("=== TaskManager Status ===");
        log.info("Total Threads: {}", totalThreads);
        log.info("Used Threads: {}", usedThreads.get());
        log.info("Available Threads: {}", totalThreads - usedThreads.get());
        
        // 重新计算当前统计信息（不累加到全局变量）
        long currentTotalTasks = totalTasks; // 已关闭执行器的累计数据
        long currentTotalCompleted = totalCompleted;
        long currentTotalRejected = totalRejected;
        long currentTotalDiscarded = totalDiscarded;
        long currentTotalCallerRuns = totalCallerRuns;
        
        if(CollectionUtils.isEmpty(executors)) {
            log.info("taskManager all actuators have timed out and been shutdown ");
        } else {
            for (TaskExecutor e : executors.values()) {
                long executorTotal = e.getTotalTaskCount();
                long executorCompleted = e.getCompletedTaskCount();
                long executorRejected = e.getRejectedTaskCount();
                long executorDiscarded = e.getDiscardedTaskCount();
                long executorCallerRuns = e.getCallerRunsTaskCount();

                // 只累加到临时变量，不修改全局变量
                currentTotalTasks += executorTotal;
                currentTotalCompleted += executorCompleted;
                currentTotalRejected += executorRejected;
                currentTotalDiscarded += executorDiscarded;
                currentTotalCallerRuns += executorCallerRuns;

                log.info("Executor [{}] - CoreSize: {}, QueueSize: {}, Rejection: {}, Active: {}, QueueUsed: {}, ExecutorCompleted: {}, Total: {}, ExecutorRejected: {}, ExecutorDiscarded: {}, CallerRuns: {}",
                        e.getName(), e.getCoreSize(), e.getQueueSize(), e.getRejectStrategy(),
                        e.getActiveCount(), e.getQueueSizeUsed(), executorCompleted, executorTotal,
                        executorRejected, executorDiscarded, executorCallerRuns);
            }
        }

        // 打印汇总信息
        log.info("=== Summary Statistics ===");
        log.info("Total Tasks Submitted: {}", currentTotalTasks);
        log.info("Total Tasks ExecutorCompleted: {}", currentTotalCompleted);
        log.info("Total Tasks ExecutorRejected: {}", currentTotalRejected);
        log.info("Total Tasks ExecutorDiscarded: {}", currentTotalDiscarded);
        log.info("Total Tasks Run by Caller: {}", currentTotalCallerRuns);
        log.info("Tasks in Progress: {}", currentTotalTasks - currentTotalCompleted - currentTotalRejected);
        
        // 计算成功率
        if (currentTotalTasks > 0) {
            DecimalFormat df = new DecimalFormat("#.##");
            double successRate = (currentTotalCompleted + currentTotalCallerRuns) * 100.0 / currentTotalTasks;
            double discardRate = currentTotalDiscarded * 100.0 / currentTotalTasks;
            log.info("Success Rate: {}% (including caller runs)", df.format(successRate));
            log.info("Discard Rate: {}%", df.format(discardRate));
        }
    }

    public void setProperties(TaskManagerConfig properties) {
        this.properties = properties;
    }
}
