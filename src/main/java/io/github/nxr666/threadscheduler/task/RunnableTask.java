
package io.github.nxr666.threadscheduler.task;

import lombok.Getter;

public abstract class RunnableTask implements Runnable {
    @Getter
    protected volatile TaskStatus status = TaskStatus.CREATED;
    @Getter
    protected long startTime;
    @Getter
    protected long endTime;
    @Getter
    protected String errorMessage;

    @Override
    public final void run() {
        try {
            startTime = System.currentTimeMillis();
            setStatus(TaskStatus.RUNNING);
            runTask();
            setStatus(TaskStatus.SUCCESS);
        } catch (Exception e) {
            setStatus(TaskStatus.FAILED);
            errorMessage = e.getMessage();
            handleException(e);
        } finally {
            endTime = System.currentTimeMillis();
        }
    }

    public void setStatus(TaskStatus status) {
        this.status = status;
    }

    protected abstract void runTask();

    protected void handleException(Exception e) {
        e.printStackTrace();
    }
}
