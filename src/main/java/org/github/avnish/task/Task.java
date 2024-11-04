package org.github.avnish.task;

import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Interface to be implemented by code that needs to be executed
 *
 * @param <T>
 */
public interface Task<T> extends Callable<T> {

    // Task Code
    public String getTaskCode();

    // get Dependent TaskCode
    public Set<String> getDependsOnTaskCodes();
}
