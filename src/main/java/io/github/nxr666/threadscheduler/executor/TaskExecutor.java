package io.github.nxr666.threadscheduler.executor;

import io.github.nxr666.threadscheduler.task.CallableTask;
import io.github.nxr666.threadscheduler.task.RunnableTask;

import java.util.concurrent.Future;

public interface TaskExecutor {
    void create();
    void execute(RunnableTask task);
    <T> Future<T> submit(CallableTask<T> task);
    void shutdown();
    String getName();
    int getCoreSize();
    int getQueueSize();
    String getRejectStrategy();
    int getActiveCount();
    int getQueueSizeUsed();
    long getCompletedTaskCount();
    void setCoreSize(int coreSize);
    
    /**
     * 检查执行器是否已关闭
     * @return 如果执行器已关闭返回true，否则返回false
     */
    boolean isShutdown();
    
    /**
     * 获取最后一次执行任务的时间戳（包括任务提交和拒绝处理时间）
     * @return 最后一次执行任务的时间戳（毫秒），如果从未执行过任务则返回创建时间
     */
        long getLastExecuteTime();
    
    /**
     * 获取总任务数（包括已完成、正在执行、队列中等待的）
     * @return 总任务数
     */
    long getTotalTaskCount();
    
    /**
     * 获取被丢弃的任务数量（DiscardPolicy和DiscardOldestPolicy）
     * @return 丢弃的任务数
     */
    long getDiscardedTaskCount();
    
    /**
     * 获取被主线程执行的任务数量（CallerRunsPolicy）
     * @return 主线程执行的任务数
     */
    long getCallerRunsTaskCount();
    
    /**
     * 获取被拒绝的任务总数（所有拒绝策略）
     * @return 拒绝的任务总数
     */
    long getRejectedTaskCount();
}
