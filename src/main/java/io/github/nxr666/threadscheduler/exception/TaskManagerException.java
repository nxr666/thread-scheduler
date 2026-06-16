package io.github.nxr666.threadscheduler.exception;

/**
 * TaskManager 自定义运行时异常
 * 用于处理任务管理器相关的配置和运行时错误
 */
public class TaskManagerException extends RuntimeException {

    public TaskManagerException(String message) {
        super(message);
    }

    public TaskManagerException(String message, Throwable cause) {
        super(message, cause);
    }

    public TaskManagerException(Throwable cause) {
        super(cause);
    }
} 