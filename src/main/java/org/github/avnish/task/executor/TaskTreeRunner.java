package org.github.avnish.task.executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.github.avnish.task.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Task Tree runner that will trigger tasks according to defined order.
 *
 * @param <T>
 */
public class TaskTreeRunner<T> {

    protected static final Logger LOGGER = LogManager.getLogger(TaskTreeRunner.class);

    private final ExecutorService executor;

    private final Map<String, TaskTree<T>> dependencyTrees = new HashMap<>();

    private Map<String, TaskTree<T>> taskCodeMap = new HashMap<>();

    private CountDownLatch latch;

    List<TaskResult<T>> taskResults = null;

    /**
     * Constructs a new TaskTreeExecutor that processes a list of tasks, organizing
     * them into dependency trees and preparing them for execution.
     *
     * @param tasks List of tasks to be executed, each containing a unique task code
     *              and a set of dependent task codes.
     */
    public TaskTreeRunner(List<Task<T>> tasks) {
        for (Task<T> task : tasks) {
            if (this.taskCodeMap.containsKey(task.getTaskCode())) {
                LOGGER.warn("Task with duplicate task has been added. It will execute only once. Consider assigning a different Task code");
            }
            taskCodeMap.put(task.getTaskCode(), new TaskTree<>(task));
        }

        for (TaskTree<T> taskTree : taskCodeMap.values()) {
            this.populateDependencyTree(taskTree, taskCodeMap, dependencyTrees);
        }

        Map<Integer, Set<String>> tasksRepresentation = taskTreeRepresentation(dependencyTrees);
        LOGGER.info("Tasks will run in blocks of {}", tasksRepresentation);

        // Get the size of largest branch in tree to size up the pool
        int size = 0;
        for (Set<String> branch : tasksRepresentation.values()) {
            if (branch.size() > size)
                size = branch.size();
        }

        // Initialize Executor for number of Trees to be Executed
        this.executor = Executors.newFixedThreadPool(size);
    }

    /**
     * Executes all tasks present in the dependency trees. The tasks are processed
     * concurrently, respecting their dependencies to ensure that parent tasks are
     * executed before their dependent child tasks.
     *
     * @return List of TaskResults
     */
    public List<TaskResult<T>> runAllTasksAndWait() throws ExecutionException {
        try {
            this.latch = new CountDownLatch(this.taskCodeMap.size());
            taskResults = new ArrayList<>(taskCodeMap.size());
            dependencyTrees.keySet().forEach(taskCode -> {
                executor.submit(new TreeExecutorTaskWrapper<>(dependencyTrees.get(taskCode), this, latch, taskResults));
            });
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            executor.shutdown();
        }
        return taskResults;
    }

    /**
     * Initiates the execution of all dependent tasks for a given parent task if the
     * provided task result indicates successful completion. If the parent task has
     * no dependent tasks or if the task result indicates an error, the method returns
     * without initiating any tasks.
     *
     * @param parentTree The parent tree for which dependencies will be kicked off
     * @param taskResult     The result of the parent task's execution used to determine
     *                       whether dependent tasks should be initiated.
     */
    public void kickOffDependentTask(TaskTree<T> parentTree, TaskResult<T> taskResult) {
        List<TaskTree<T>> dependencies = parentTree.getChildTaskTree();
        if(dependencies == null ||
                dependencies.isEmpty()) {
            return;
        }
        boolean hasErrored = taskResult.isErrored() || taskResult.getStatus() == TaskRunStatus.ERRORED;
        synchronized (this.dependencyTrees) {
            for (TaskTree<T> dependencyTree : dependencies) {
                // Remove parent link as it already complete
                dependencyTree.removeParent(parentTree);

                if (hasErrored || TaskRunStatus.NOT_RUNNABLE.equals(taskResult.getStatus())) {
                    // mark the underlying child task as not runnable
                    dependencyTree.setCanRun(false);
                }

                // If there are more parents, return. The next parent will kick off the dependent task.
                if (dependencyTree.hasParents()) {
                    continue;
                }
                this.executor.submit(new TreeExecutorTaskWrapper<>(dependencyTree, this, this.latch, taskResults));
            }
        }
    }

    /**
     * Get dependencyTree
     *
     * @return
     */
    public Map<String, TaskTree<T>> getDependenciesTree() {
        return this.dependencyTrees;
    }

    /**
     * Method to Print Task Tree invocation order
     *
     * @param dependencyTrees
     * @return
     */
    private Map<Integer, Set<String>> taskTreeRepresentation(Map<String, TaskTree<T>> dependencyTrees) {
        Map<Integer, Set<String>> map = new HashMap<>();
        Queue<String> queue = new LinkedList<>();
        for (Map.Entry<String, TaskTree<T>> entry : dependencyTrees.entrySet()) {
            Integer index = 1;
            append(map, index, entry.getValue());
        }
        return map;
    }

    public void append(Map<Integer, Set<String>> map, Integer index, TaskTree<T> taskTree) {
        Set<String> set = map.computeIfAbsent(index, k -> new HashSet<>());
        set.add(taskTree.getWorkerTask().getTaskCode());
        if (taskTree.hasChildren()) {
            for (TaskTree<T> child : taskTree.getChildTaskTree()) {
                index++;
                append(map, index, child);
            }
        }
    }

    // Recursively create dependency trees
    private void populateDependencyTree(TaskTree<T> currentTaskTree,
                                        Map<String, TaskTree<T>> taskCodeMap,
                                        Map<String, TaskTree<T>> dependencyTree) {

        Task<T> currentTask = currentTaskTree.getWorkerTask();
        // Only add to dependency tree if task is not dependent on any other task
        if (currentTask.getDependsOnTaskCodes() == null || currentTask.getDependsOnTaskCodes().isEmpty()) {
            dependencyTree.putIfAbsent(currentTask.getTaskCode(), currentTaskTree);
        }

        for (String parentTaskCode : currentTask.getDependsOnTaskCodes()) {
            TaskTree<T> parentTaskTree = taskCodeMap.get(parentTaskCode);
            parentTaskTree.addChildTaskTree(currentTaskTree);
        }
    }
}


