package org.github.avnish.task;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.github.avnish.task.executor.TaskTreeRunner;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TaskTreeExecutorTest {

    protected static final Logger LOGGER = LogManager.getLogger(TaskTreeExecutorTest.class);

    @Test
    public void testExecuteAll() throws ExecutionException {

        Task<Boolean> task1 = new Task<Boolean>() {
            @Override
            public String getTaskCode() {
                return "TASK1";
            }

            @Override
            public Set<String> getDependsOnTaskCodes() {
                return Set.of();
            }

            @Override
            public Boolean call() throws Exception {
                Thread.sleep(50);
                LOGGER.info("executing task {}", getTaskCode());
                return true;
            }
        };

        Task<Boolean> task2 = new Task<Boolean>() {
            @Override
            public String getTaskCode() {
                return "TASK2";
            }

            @Override
            public Set<String> getDependsOnTaskCodes() {
                return Set.of();
            }

            @Override
            public Boolean call() throws Exception {
                Thread.sleep(50);
                LOGGER.info("executing task {}", getTaskCode());
                return true;
            }
        };

        Task<Boolean> task3 = new Task<Boolean>() {
            @Override
            public String getTaskCode() {
                return "TASK3";
            }

            @Override
            public Set<String> getDependsOnTaskCodes() {
                return Set.of("TASK1", "TASK2");
            }

            @Override
            public Boolean call() throws Exception {
                Thread.sleep(50);
                LOGGER.info("executing task {}", getTaskCode());
                return true;
            }
        };

        TaskTreeRunner<Boolean> executor = new TaskTreeRunner<>(List.of(task1, task2, task3));
        Map<String, TaskTree<Boolean>> tree = executor.getDependenciesTree();
        Assertions.assertEquals(2, tree.size());

        List<TaskResult<Boolean>> results = executor.runAllTasksAndWait();
        results.stream().filter(result -> result.getTaskCode().equals("TASK1")).findFirst().orElse(null).getStatus().equals(TaskRunStatus.COMPLETED);
        results.stream().filter(result -> result.getTaskCode().equals("TASK2")).findFirst().orElse(null).getStatus().equals(TaskRunStatus.COMPLETED);
        results.stream().filter(result -> result.getTaskCode().equals("TASK3")).findFirst().orElse(null).getStatus().equals(TaskRunStatus.COMPLETED);
    }

    @Test
    public void testExecuteAllWithDeepTree() throws ExecutionException {

        Task<Boolean> task1 = new Task<Boolean>() {
            @Override
            public String getTaskCode() {
                return "TASK1";
            }

            @Override
            public Set<String> getDependsOnTaskCodes() {
                return Set.of();
            }

            @Override
            public Boolean call() throws Exception {
                Thread.sleep(50);
                LOGGER.info("executing task {}", getTaskCode());
                return true;
            }
        };

        Task<Boolean> task2 = new Task<Boolean>() {
            @Override
            public String getTaskCode() {
                return "TASK2";
            }

            @Override
            public Set<String> getDependsOnTaskCodes() {
                return Set.of("TASK1");
            }

            @Override
            public Boolean call() throws Exception {
                Thread.sleep(50);
                LOGGER.info("executing task {}", getTaskCode());
                return true;
            }
        };

        Task<Boolean> task3 = new Task<Boolean>() {
            @Override
            public String getTaskCode() {
                return "TASK3";
            }

            @Override
            public Set<String> getDependsOnTaskCodes() {
                return Set.of("TASK2");
            }

            @Override
            public Boolean call() throws Exception {
                Thread.sleep(50);
                LOGGER.info("executing task {}", getTaskCode());
                return true;
            }
        };
        TaskTreeRunner<Boolean> executor = new TaskTreeRunner<>(List.of(task2, task1, task3));
        Map<String, TaskTree<Boolean>> tree = executor.getDependenciesTree();
        Assertions.assertEquals(1, tree.size());
        List<TaskResult<Boolean>> results = executor.runAllTasksAndWait();
        results.stream().filter(result -> result.getTaskCode().equals("TASK1")).findFirst().orElse(null).getStatus().equals(TaskRunStatus.COMPLETED);
        results.stream().filter(result -> result.getTaskCode().equals("TASK2")).findFirst().orElse(null).getStatus().equals(TaskRunStatus.COMPLETED);
        results.stream().filter(result -> result.getTaskCode().equals("TASK3")).findFirst().orElse(null).getStatus().equals(TaskRunStatus.COMPLETED);
    }


    @Test
    public void testExecuteAllWithDeepTreeException() throws ExecutionException {

        Task<Boolean> task1 = new Task<Boolean>() {
            @Override
            public String getTaskCode() {
                return "TASK1";
            }

            @Override
            public Set<String> getDependsOnTaskCodes() {
                return Set.of();
            }

            @Override
            public Boolean call() throws Exception {
                Thread.sleep(50);
                LOGGER.info("executing task {}", getTaskCode());
                return true;
            }
        };

        Task<Boolean> task2 = new Task<Boolean>() {
            @Override
            public String getTaskCode() {
                return "TASK2";
            }

            @Override
            public Set<String> getDependsOnTaskCodes() {
                return Set.of("TASK1");
            }

            @Override
            public Boolean call() throws Exception {
                Thread.sleep(50);
                throw new RuntimeException("Testing exception");
            }
        };

        Task<Boolean> task3 = new Task<Boolean>() {
            @Override
            public String getTaskCode() {
                return "TASK3";
            }

            @Override
            public Set<String> getDependsOnTaskCodes() {
                return Set.of("TASK2");
            }

            @Override
            public Boolean call() throws Exception {
                Thread.sleep(50);
                LOGGER.info("executing task {}", getTaskCode());
                return true;
            }
        };
        TaskTreeRunner<Boolean> executor = null;
        executor = new TaskTreeRunner<>(List.of(task3, task2, task1));

        List<TaskResult<Boolean>> results = executor.runAllTasksAndWait();
        results.stream().filter(result -> result.getTaskCode().equals("TASK1")).findFirst().orElse(null).getStatus().equals(TaskRunStatus.COMPLETED);
        results.stream().filter(result -> result.getTaskCode().equals("TASK2")).findFirst().orElse(null).getStatus().equals(TaskRunStatus.ERRORED);
        results.stream().filter(result -> result.getTaskCode().equals("TASK2")).findFirst().orElse(null).getThrowable().getMessage().equals("Testing exception");
        results.stream().filter(result -> result.getTaskCode().equals("TASK3")).findFirst().orElse(null).getStatus().equals(TaskRunStatus.NOT_RUNNABLE);
    }
}
