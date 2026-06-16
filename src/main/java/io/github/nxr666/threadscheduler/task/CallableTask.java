
package io.github.nxr666.threadscheduler.task;

import lombok.Getter;
import java.util.concurrent.Callable;

public abstract class CallableTask<T> implements Callable<T> {
    @Getter
    protected volatile TaskStatus status = TaskStatus.CREATED;
    @Getter
    protected long startTime;
    @Getter
    protected long endTime;
    @Getter
    protected String errorMessage;

    @Override
    public final T call() throws Exception {
        try {
            startTime = System.currentTimeMillis();
            setStatus(TaskStatus.RUNNING);
            T result = callTask();
            setStatus(TaskStatus.SUCCESS);
            return result;
        } catch (Exception e) {
            setStatus(TaskStatus.FAILED);
            errorMessage = e.getMessage();
            throw e;
        } finally {
            endTime = System.currentTimeMillis();
        }
    }

    public void setStatus(TaskStatus status) {
        this.status = status;
    }

    protected abstract T callTask() throws Exception;
}
