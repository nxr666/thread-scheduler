package io.github.nxr666.threadscheduler.executor;

import lombok.extern.slf4j.Slf4j;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 带统计功能的拒绝执行处理器
 * 包装原有的拒绝策略，同时统计各种拒绝情况
 */
@Slf4j
public class StatisticsRejectedExecutionHandler implements RejectedExecutionHandler {
    
    private final RejectedExecutionHandler delegate;
    private final String strategyName;
    private final String executorName;
    private final AtomicLong rejectedTaskCount = new AtomicLong(0);
    private final AtomicLong discardedTaskCount = new AtomicLong(0);
    private final AtomicLong callerRunsTaskCount = new AtomicLong(0);
    private final Runnable executeTimeCallback;
    
    public StatisticsRejectedExecutionHandler(RejectedExecutionHandler delegate, String strategyName, String executorName) {
        this(delegate, strategyName, executorName, null);
    }
    
    public StatisticsRejectedExecutionHandler(RejectedExecutionHandler delegate, String strategyName, String executorName, Runnable executeTimeCallback) {
        this.delegate = delegate;
        this.strategyName = strategyName;
        this.executorName = executorName;
        this.executeTimeCallback = executeTimeCallback;
    }
    
    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        rejectedTaskCount.incrementAndGet();
        
        // 更新执行时间（表示执行器仍然活跃，正在处理拒绝策略）
        if (executeTimeCallback != null) {
            executeTimeCallback.run();
        }
        
        // 根据策略类型统计
        switch (strategyName.toLowerCase()) {
            case "discardpolicy":
                discardedTaskCount.incrementAndGet();
                log.debug("执行器 [{}] 任务被丢弃 (DiscardPolicy)", executorName);
                break;
            case "discardoldestpolicy":
                discardedTaskCount.incrementAndGet();
                log.debug("执行器 [{}] 任务被丢弃 (DiscardOldestPolicy)", executorName);
                break;
            case "callerrunspolicy":
                callerRunsTaskCount.incrementAndGet();
                log.debug("执行器 [{}] 任务将在调用线程执行 (CallerRunsPolicy)", executorName);
                break;
            case "abortpolicy":
                log.debug("执行器 [{}] 任务被拒绝，抛出异常 (AbortPolicy)", executorName);
                break;
            default:
                log.debug("执行器 [{}] 任务被拒绝，未知策略: {}", executorName, strategyName);
                break;
        }
        
        // 委托给原有的处理器
        delegate.rejectedExecution(r, executor);
    }
    
    /**
     * 获取被拒绝的任务总数
     */
    public long getRejectedTaskCount() {
        return rejectedTaskCount.get();
    }
    
    /**
     * 获取被丢弃的任务数量
     */
    public long getDiscardedTaskCount() {
        return discardedTaskCount.get();
    }
    
    /**
     * 获取由调用线程执行的任务数量
     */
    public long getCallerRunsTaskCount() {
        return callerRunsTaskCount.get();
    }
    
    /**
     * 重置统计计数器
     */
    public void resetCounters() {
        rejectedTaskCount.set(0);
        discardedTaskCount.set(0);
        callerRunsTaskCount.set(0);
    }
} 