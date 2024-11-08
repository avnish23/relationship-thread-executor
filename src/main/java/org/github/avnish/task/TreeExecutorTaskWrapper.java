package org.github.avnish.task;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.github.avnish.task.executor.TaskTreeRunner;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

/**
 * Wrapper class for Actual callable Object
 *
 * @param <T>
 */
public class TreeExecutorTaskWrapper<T> implements Callable<TaskResult<T>> {

    protected static final Logger LOGGER = LogManager.getLogger(TaskTreeRunner.class);

    private final TaskTree<T> tasktree;

    private final TaskTreeRunner<T> executor;

    private final CountDownLatch latch;

    private final List<TaskResult<T>> resultsAccumulator;

    public TreeExecutorTaskWrapper(TaskTree<T> tasktree, TaskTreeRunner<T> executor, CountDownLatch latch, List<TaskResult<T>> resultsAccumulator) {
        this.tasktree = tasktree;
        this.executor = executor;
        this.latch = latch;
        this.resultsAccumulator = resultsAccumulator;
    }

    @Override
    public TaskResult<T> call() throws Exception {
        TaskResult<T> taskResult = null;
        try {
            if (!this.tasktree.isCanRun()) {
                LOGGER.info("Task {} has been marked as Not Runnable", this.tasktree.getWorkerTask().getTaskCode());
                taskResult = new TaskResult<>(null, TaskRunStatus.NOT_RUNNABLE, this.tasktree.getWorkerTaskCode());
            } else {
                T result = this.tasktree.getWorkerTask().call();
                taskResult = new TaskResult<>(result, TaskRunStatus.COMPLETED, this.tasktree.getWorkerTaskCode());
                resultsAccumulator.add(taskResult);
            }
        } catch (Exception e) {
            taskResult = new TaskResult<>(e, this.tasktree.getWorkerTaskCode());
        } finally {
            latch.countDown();
            executor.kickOffDependentTask(this.tasktree, taskResult);
        }
        resultsAccumulator.add(taskResult);
        return taskResult;
    }
}
