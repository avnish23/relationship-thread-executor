package org.github.avnish.task;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.github.avnish.task.executor.TaskTreeRunner;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

public class TaskTreeExecutorTest {

    protected static final Logger LOGGER = LogManager.getLogger(TaskTreeExecutorTest.class);

    @Test
    public void testExecuteAll() {

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
                LOGGER.info("executing task {}", getTaskCode());
                return true;
            }
        };

        TaskTreeRunner<Boolean> executor = new TaskTreeRunner<>(List.of(task1, task2, task3));
        executor.runAllTasks();
    }

    @Test
    public void testExecuteAllWithDeepTree() {

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
                return true;
            }
        };
        TaskTreeRunner<Boolean> executor = new TaskTreeRunner<>(List.of(task1, task2, task3));
        executor.runAllTasks();
    }
}
