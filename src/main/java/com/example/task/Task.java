package com.example.task;

import java.util.Set;
import java.util.concurrent.Callable;

public interface Task<T> extends Callable<T> {

    // Task Code
    public String getTaskCode();

    // get Dependent TaskCode
    public Set<String> getDependsOnTaskCodes();
}
