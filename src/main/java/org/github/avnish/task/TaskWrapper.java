package org.github.avnish.task;

import java.util.concurrent.Callable;

public abstract class TaskWrapper<T> implements Callable<T> {

    public abstract T call();

}