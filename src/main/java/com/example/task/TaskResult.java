package com.example.task;

public class TaskResult<T> {

    private T result;

    private TaskRunStatus status;

    private Throwable throwable;

    public TaskResult(T result, TaskRunStatus status) {
        this.result = result;
        this.status = status;
    }

    public TaskResult(Throwable throwable) {
        this.throwable = throwable;
        this.status = TaskRunStatus.ERRORED;
    }

    public boolean isErrored() {
        return status == TaskRunStatus.ERRORED;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public void setThrowable(Throwable throwable) {
        this.throwable = throwable;
    }

    public TaskRunStatus getStatus() {
        return status;
    }

    public void setStatus(TaskRunStatus status) {
        this.status = status;
    }

    public T getResult() {
        return result;
    }

    public void setResult(T result) {
        this.result = result;
    }
}
