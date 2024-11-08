# relationship-thread-executor
## Description:
Thread Executor that can run worker tasks based on defined relationships.

Tasks at same level in tree will run in parallel and will trigger dependent Tasks.
Dependent Tasks are only kicked off if parents are success or else these will skipped

This allows tasks to be executed in most optimized manner 
