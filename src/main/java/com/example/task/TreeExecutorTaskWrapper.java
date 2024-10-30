package com.example.task;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Callable;

/**
 * Wrapper class for Actual callable Object
 *
 * @param <T>
 */
public class TreeExecutorTaskWrapper<T> implements Callable<TaskResult<T>> {

    protected static final Logger LOGGER = LogManager.getLogger(TaskTreeExecutor.class);

    private final TaskTree<T> tasktree;

    private final TaskTreeExecutor<T> executor;

    public TreeExecutorTaskWrapper(TaskTree<T> tasktree, TaskTreeExecutor<T> executor) {
        this.tasktree = tasktree;
        this.executor = executor;
    }


    @Override
    public TaskResult<T> call() throws Exception {
        if (this.tasktree.isCanRun()) {
            LOGGER.info("Task {} has been marked as Not Runnable", this.tasktree.getWorkerTask().getTaskCode());
            return new TaskResult<>(null, TaskRunStatus.NOT_RUNNABLE);
        }
        TaskResult<T> taskResult = null;
        try {
            T result = this.tasktree.getWorkerTask().call();
            taskResult = new TaskResult<>(result, TaskRunStatus.COMPLETED);
        } catch (Exception e) {
            taskResult = new TaskResult<>(e);
        } finally {
            executor.kickOffDependentTask(this.tasktree, taskResult);
        }
        return taskResult;
    }
}
