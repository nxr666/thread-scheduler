package io.github.nxr666.threadscheduler.executor;

import io.github.nxr666.threadscheduler.task.CallableTask;
import io.github.nxr666.threadscheduler.task.RunnableTask;
import io.github.nxr666.threadscheduler.exception.ExecutorClosedException;
import lombok.extern.slf4j.Slf4j;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class GenericTaskExecutor implements TaskExecutor {
    private final String name;
    private final int queueSize;
    private final String rejectStrategy;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicLong lastExecuteTime = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong totalTaskCount = new AtomicLong(0);
    private ThreadPoolExecutor executor;
    private StatisticsRejectedExecutionHandler statisticsHandler;
    private int coreSize;

    public GenericTaskExecutor(String name, int coreSize, int queueSize, String rejectStrategy) {
        this.name = name;
        this.coreSize = coreSize;
        this.queueSize = queueSize;
        this.rejectStrategy = rejectStrategy;
    }

    @Override
    public void create() {
        if (started.compareAndSet(false, true)) {
            BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>(queueSize);
            RejectedExecutionHandler originalHandler = createHandler(rejectStrategy);
            
            // 使用统计拒绝处理器包装原有处理器，传入执行时间回调
            this.statisticsHandler = new StatisticsRejectedExecutionHandler(originalHandler, rejectStrategy, name, 
                () -> lastExecuteTime.set(System.currentTimeMillis()));
            
            this.executor = new ThreadPoolExecutor(coreSize, coreSize, 60L, TimeUnit.SECONDS, workQueue,
                r -> {
                    Thread t = Executors.defaultThreadFactory().newThread(r);
                    t.setName("executor-" + name + "-" + t.getId());
                    return t;
                },
                statisticsHandler) {
                
                @Override
                protected void afterExecute(Runnable r, Throwable t) {
                    super.afterExecute(r, t);
                    // 更新最后执行时间
                    lastExecuteTime.set(System.currentTimeMillis());
                }
            };
            log.info("Created executor '{}'", name);
        }
    }

    @Override
    public void execute(RunnableTask task) {
        // 更新最后执行时间
        lastExecuteTime.set(System.currentTimeMillis());
        // 增加总任务计数
        totalTaskCount.incrementAndGet();
        try {
            executor.execute(task);
        } catch (RejectedExecutionException e) {
            // 检查异常原因：只有执行器关闭时才抛出ExecutorClosedException
            if (executor.isShutdown()) {
                throw new ExecutorClosedException(name, "execute", e);
            } else {
                // 其他原因（如队列满了触发拒绝策略）重新抛出原异常
                throw e;
            }
        }
    }

    @Override
    public <T> Future<T> submit(CallableTask<T> task) {
        // 更新最后执行时间
        lastExecuteTime.set(System.currentTimeMillis());
        // 增加总任务计数
        totalTaskCount.incrementAndGet();
        try {
            return executor.submit(task);
        } catch (RejectedExecutionException e) {
            // 检查异常原因：只有执行器关闭时才抛出ExecutorClosedException
            if (executor.isShutdown()) {
                throw new ExecutorClosedException(name, "submit", e);
            } else {
                // 其他原因（如队列满了触发拒绝策略）重新抛出原异常
                throw e;
            }
        }
    }

    @Override
    public void shutdown() {
        executor.shutdownNow();
        log.info("Executor '{}' shutdown", name);
    }

    @Override
    public String getName() { return name; }

    @Override
    public int getCoreSize() { return coreSize; }

    @Override
    public int getQueueSize() { return queueSize; }

    @Override
    public String getRejectStrategy() { return rejectStrategy; }

    @Override
    public int getActiveCount() { return executor.getActiveCount(); }

    @Override
    public int getQueueSizeUsed() { return executor.getQueue().size(); }

    @Override
    public long getCompletedTaskCount() { return executor.getCompletedTaskCount(); }

    @Override
    public long getLastExecuteTime() {
        return lastExecuteTime.get();
        }

    @Override
    public void setCoreSize(int coreSize) {
        this.coreSize = coreSize;
        executor.setCorePoolSize(coreSize);
        executor.setMaximumPoolSize(coreSize);
        log.info("Updated executor '{}' core size to {}", name, coreSize);
    }

    @Override
    public boolean isShutdown() {
        return executor != null && executor.isShutdown();
    }

    @Override
    public long getTotalTaskCount() {
        return totalTaskCount.get();
    }

    @Override
    public long getDiscardedTaskCount() {
        return statisticsHandler != null ? statisticsHandler.getDiscardedTaskCount() : 0;
    }

    @Override
    public long getCallerRunsTaskCount() {
        return statisticsHandler != null ? statisticsHandler.getCallerRunsTaskCount() : 0;
    }

    @Override
    public long getRejectedTaskCount() {
        return statisticsHandler != null ? statisticsHandler.getRejectedTaskCount() : 0;
    }

    private RejectedExecutionHandler createHandler(String policy) {
        if (policy == null) {
            return new ThreadPoolExecutor.CallerRunsPolicy();
        }
        
        // 忽略大小写进行策略匹配
        String normalizedPolicy = policy.trim();
        if ("AbortPolicy".equalsIgnoreCase(normalizedPolicy)) {
            return new ThreadPoolExecutor.AbortPolicy();
        } else if ("DiscardPolicy".equalsIgnoreCase(normalizedPolicy)) {
            return new ThreadPoolExecutor.DiscardPolicy();
        } else if ("DiscardOldestPolicy".equalsIgnoreCase(normalizedPolicy)) {
            return new ThreadPoolExecutor.DiscardOldestPolicy();
        } else if ("CallerRunsPolicy".equalsIgnoreCase(normalizedPolicy)) {
            return new ThreadPoolExecutor.CallerRunsPolicy();
        } else {
            log.warn("Unknown reject policy '{}', using default CallerRunsPolicy", policy);
            return new ThreadPoolExecutor.CallerRunsPolicy();
        }
    }
}
