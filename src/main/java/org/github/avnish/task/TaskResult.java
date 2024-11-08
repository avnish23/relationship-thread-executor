package org.github.avnish.task;

public class TaskResult<T> {

    private T result;

    private TaskRunStatus status;

    private Throwable throwable;

    private String taskCode;

    public TaskResult(T result, TaskRunStatus status, String taskCode) {
        this.result = result;
        this.status = status;
        this.taskCode = taskCode;
    }

    public TaskResult(Throwable throwable, String taskCode) {
        this.throwable = throwable;
        this.status = TaskRunStatus.ERRORED;
        this.taskCode = taskCode;
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

    public String getTaskCode() {
        return this.taskCode;
    }

    public void setTaskCode(String taskCode) {
        this.taskCode = taskCode;
    }
}
