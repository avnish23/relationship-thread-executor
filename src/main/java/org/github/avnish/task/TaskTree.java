package org.github.avnish.task;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Represents a task within a tree structure where each task can have a parent task
 * and multiple dependent child tasks. The parent task must be completed before
 * its dependent tasks.
 *
 * @param <T> The type of data associated with the task.
 */
public class TaskTree<T> {

    // Worker Task
    private Task<T> workerTask;

    // parent Tasks for this Task. Multiple Tasks could be required before executing this task
    private List<TaskTree<T>> parentTasks;

    // Get dependent tasks
    private List<TaskTree<T>> childTasks;

    // Flag to check if a task can run. This will be set to False if a parent task has errored.
    private boolean canRun = true;

    /**
     * Create a Task Tree Object
     *
     * @param workerTask
     */
    public TaskTree(Task<T> workerTask) {
        this.workerTask = workerTask;
    }

    /**
     * Retrieves the worker task associated with this task tree.
     *
     * @return The worker task of type {@code Task<T>}.
     */
    public Task<T> getWorkerTask() {
        return this.workerTask;
    }

    /**
     * Sets the parent task for this task tree.
     *
     * @param parentTask The task tree that represents the parent task.
     */
    public void addToParentTask(TaskTree<T> parentTask) {
        if (this.parentTasks == null) {
            this.parentTasks = new ArrayList<>();
        }
        this.parentTasks.add(parentTask);
    }

    /**
     * Adds a child task tree to the current task tree. This method sets up a
     * parent-child relationship between the current task tree and the specified
     * child task tree.
     *
     * @param taskTree The task tree that is to be added as a child to the
     * current task tree. If the task tree is null or does not have a worker
     * task, the method returns without making any changes.
     */
    public void addChildTaskTree(TaskTree<T> taskTree) {
        if (taskTree == null || taskTree.getWorkerTask() == null) {
            return;
        }
        if (this.childTasks == null) {
            this.childTasks = new ArrayList<>();
        }
        this.childTasks.add(taskTree);
        taskTree.addToParentTask(this);
    }

    /**
     * Retrieves the list of child task trees associated with this task tree.
     *
     * @return A list of {@code TaskTree<T>} instances representing the child tasks.
     *         If there are no child tasks, this method returns {@code null}.
     */
    public List<TaskTree<T>> getChildTaskTree() {
        return this.childTasks;
    }

    /**
     * Removes the specified parent task tree from the list of parent tasks
     * associated with this task tree.
     *
     * @param parentTree The task tree to be removed from the list of parent
     *                   tasks of this task tree.
     */
    public void removeParent(TaskTree<T> parentTree) {
        this.parentTasks.remove(parentTree);
    }

    /**
     * Checks if the current task tree has any parent tasks assigned.
     *
     * @return {@code true} if the current task tree has no parent tasks,
     *         otherwise {@code false}.
     */
    public boolean hasParents() {
        return this.parentTasks != null && !this.parentTasks.isEmpty();
    }

    /**
     * Checks if the current task tree has any parent tasks assigned.
     *
     * @return {@code true} if the current task tree has no parent tasks,
     *         otherwise {@code false}.
     */
    public List<TaskTree<T>>  getParents() {
        return this.parentTasks;
    }

    /**
     *
     * @return taskCode
     */
    public String getWorkerTaskCode() {
        return this.workerTask.getTaskCode();
    }

    /**
     *
     * @return
     */
    public boolean hasChildren() {
        return this.childTasks != null && !this.childTasks.isEmpty();
    }

    /**
     * String representation for POJO
     *
     * @return
     */
    public String toString() {
        return this.workerTask.getTaskCode();
    }

    /**
     * Equals implementation
     *
     * @param taskTree
     * @return
     */
    public boolean equals(TaskTree<T> taskTree) {
        return this.workerTask.getTaskCode().equals(taskTree.getWorkerTask().getTaskCode());
    }

    /**
     *
     * @return
     */
    public boolean isCanRun() {
        return canRun;
    }

    /**
     *
     * @param canRun
     */
    public void setCanRun(boolean canRun) {
        this.canRun = canRun;
    }

}
