package org.github.avnish.task.executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.github.avnish.task.Task;
import org.github.avnish.task.TaskResult;
import org.github.avnish.task.TaskTree;
import org.github.avnish.task.TreeExecutorTaskWrapper;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 *
 * @param <T>
 */
public class TaskTreeRunner<T> {

    protected static final Logger LOGGER = LogManager.getLogger(TaskTreeRunner.class);

    private ExecutorService executor;

    private final Map<String, TaskTree<T>> dependencyTrees = new HashMap<>();

    private final Map<String, TaskTree<T>> taskCodeMap = new HashMap<>();


    /**
     * Constructs a new TaskTreeExecutor that processes a list of tasks, organizing
     * them into dependency trees and preparing them for execution.
     *
     * @param tasks List of tasks to be executed, each containing a unique task code
     *              and a set of dependent task codes.
     */
    public TaskTreeRunner(List<Task<T>> tasks) {

        // map out the list with Task Code
        for (Task<T> task : tasks) {
            taskCodeMap.put(task.getTaskCode(), new TaskTree<>(task));
        }

        for (TaskTree<T> taskTree : taskCodeMap.values()) {
            this.populateDependencyTree(taskTree, taskCodeMap, dependencyTrees);
        }

        LOGGER.info(printTaskTree(dependencyTrees));

        // Initialize Executor for number of Trees to be Executed
        executor = Executors.newFixedThreadPool(dependencyTrees.size());
    }



    /**
     * Executes all tasks present in the dependency trees. The tasks are processed
     * concurrently, respecting their dependencies to ensure that parent tasks are
     * executed before their dependent child tasks.
     */
    public List<Future<TaskResult<T>>> runAllTasks() {
        List<Future<TaskResult<T>>> futures = new ArrayList<>();
        try {
            dependencyTrees.keySet().forEach(taskCode -> {
                futures.add(executor.submit(new TreeExecutorTaskWrapper<>(dependencyTrees.get(taskCode), this)));
            });
        } finally {
            executor.shutdown();
        }
        return futures;
    }

    /**
     * Executes all tasks present in the dependency trees. The tasks are processed
     * concurrently, respecting their dependencies to ensure that parent tasks are
     * executed before their dependent child tasks.
     *
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public List<TaskResult<T>> runAllTasksAndWait() throws ExecutionException, InterruptedException {
        List<Future<TaskResult<T>>> futures = this.runAllTasks();

        List<TaskResult<T>> taskResults = new ArrayList<>(futures.size());
        for (Future<TaskResult<T>> future : futures) {
            taskResults.add(future.get());
        }
        return taskResults;
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
        boolean hasErrored = taskResult.isErrored();
        synchronized (this.dependencyTrees) {
            for (TaskTree<T> dependencyTree : dependencies) {
                // Remove parent link as it already complete
                dependencyTree.removeParent(parentTree);

                // mark the underlying child task as not runnable
                dependencyTree.setCanRun(false);

                // If there are more parents, return. The next parent will kick off the dependent task.
                if (dependencyTree.hasParents()) {
                    continue;
                }

                this.executor.submit(new TreeExecutorTaskWrapper<>(dependencyTree, this));
            }
        }

    }

    private String printTaskTree(Map<String, TaskTree<T>> dependencyTrees) {
        Map<Integer, Set<String>> map = new HashMap<>();
        Queue<String> queue = new LinkedList<>();
        for (Map.Entry<String, TaskTree<T>> entry : dependencyTrees.entrySet()) {
            Integer index = 1;
            append(map, index, entry.getValue());
        }
        return map.toString();
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
}


