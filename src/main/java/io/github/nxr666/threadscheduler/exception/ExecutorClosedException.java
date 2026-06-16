package io.github.nxr666.threadscheduler.exception;

/**
 * 执行器已关闭异常
 * 当尝试向已关闭的执行器提交任务时抛出此异常
 */
public class ExecutorClosedException extends RuntimeException {
    
    private final String executorName;
    private final String operation;
    
    /**
     * 构造函数
     * @param executorName 执行器名称
     * @param operation 操作类型（execute/submit）
     */
    public ExecutorClosedException(String executorName, String operation) {
        super(String.format("执行器 [%s] 已关闭，无法执行 %s 操作", executorName, operation));
        this.executorName = executorName;
        this.operation = operation;
    }
    
    /**
     * 构造函数，包含原因
     * @param executorName 执行器名称
     * @param operation 操作类型
     * @param cause 原因异常
     */
    public ExecutorClosedException(String executorName, String operation, Throwable cause) {
        super(String.format("执行器 [%s] 已关闭，无法执行 %s 操作", executorName, operation), cause);
        this.executorName = executorName;
        this.operation = operation;
    }
    
    /**
     * 获取执行器名称
     * @return 执行器名称
     */
    public String getExecutorName() {
        return executorName;
    }
    
    /**
     * 获取操作类型
     * @return 操作类型
     */
    public String getOperation() {
        return operation;
    }
} 