package io.github.nxr666.threadscheduler.core;

import io.github.nxr666.threadscheduler.task.CallableTask;
import io.github.nxr666.threadscheduler.task.RunnableTask;

import java.util.concurrent.Future;

public interface TaskManager {
    void execute(String executorName, RunnableTask task);
    <T> Future<T> submit(String executorName, CallableTask<T> task);
    void shutdown(String executorName);
}
