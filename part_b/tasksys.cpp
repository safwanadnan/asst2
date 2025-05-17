#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    this->num_threads = num_threads;
    should_terminate = false;
    next_task_id = 1; // Start from 1 to avoid 0 as a valid task ID
    total_tasks_remaining = 0;
    
    // Create the thread pool
    for (int i = 0; i < num_threads; i++) {
        thread_pool.push_back(std::thread([this]() {
            while (true) {
                // Get a task from the queue or wait if none available
                TaskID task_id = 0;
                int task_index = -1;
                int num_total_tasks = 0;
                
                {
                    std::unique_lock<std::mutex> lock(queue_mutex);
                    
                    // Wait until there's a task available or termination is requested
                    cv_tasks_available.wait(lock, [this]() {
                        return !ready_queue.empty() || should_terminate;
                    });
                    
                    // Exit if termination is requested
                    if (should_terminate) {
                        break;
                    }
                    
                    // Get a task from the ready queue
                    if (!ready_queue.empty()) {
                        auto task = ready_queue.front();
                        ready_queue.pop();
                        
                        task_id = std::get<0>(task);
                        task_index = std::get<1>(task);
                        num_total_tasks = std::get<2>(task);
                    }
                }
                
                // If we got a valid task, execute it
                if (task_id > 0) {
                    auto& task = task_info[task_id];
                    
                    // Execute the task
                    task.runnable->runTask(task_index, num_total_tasks);
                    
                    // Update task completion status
                    {
                        std::lock_guard<std::mutex> lock(queue_mutex);
                        
                        // Decrement the counter of remaining tasks for this bulk launch
                        task.tasks_remaining--;
                        total_tasks_remaining--;
                        
                        // If this is the last task of the bulk launch, update dependencies
                        if (task.tasks_remaining == 0) {
                            task.is_complete = true;
                            
                            // Check if any waiting task can now be executed
                            checkAndEnqueueWaitingTasks(task_id);
                        }
                        
                        // If no more tasks are running, notify sync()
                        if (total_tasks_remaining == 0) {
                            cv_tasks_done.notify_all();
                        }
                    }
                }
            }
        }));
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    // Signal threads to terminate
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        should_terminate = true;
    }
    
    // Wake up all worker threads
    cv_tasks_available.notify_all();
    
    // Join all the threads
    for (auto& thread : thread_pool) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::checkAndEnqueueWaitingTasks(TaskID completed_task_id) {
    // Find tasks that were waiting on this completed task
    for (auto it = waiting_tasks.begin(); it != waiting_tasks.end(); ) {
        TaskID waiting_task_id = it->first;
        std::set<TaskID>& dependencies = it->second;
        
        // Remove the completed task from the dependencies
        dependencies.erase(completed_task_id);
        
        // If all dependencies are satisfied, move tasks to the ready queue
        if (dependencies.empty()) {
            // This task has no more dependencies, add all its subtasks to the ready queue
            auto& task = task_info[waiting_task_id];
            for (int i = 0; i < task.num_total_tasks; i++) {
                ready_queue.push(std::make_tuple(waiting_task_id, i, task.num_total_tasks));
            }
            
            // Remove this task from the waiting list
            it = waiting_tasks.erase(it);
            
            // Notify worker threads that new tasks are available
            cv_tasks_available.notify_all();
        } else {
            ++it;
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    // We can implement run() using runAsyncWithDeps() and sync()
    std::vector<TaskID> no_deps;
    TaskID task_id = runAsyncWithDeps(runnable, num_total_tasks, no_deps);
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    // If there are no tasks, return early
    if (num_total_tasks == 0) {
        return next_task_id++;
    }
    
    // Lock the mutex to update shared data structures
    std::unique_lock<std::mutex> lock(queue_mutex);
    
    // Get a new task ID
    TaskID new_task_id = next_task_id++;
    
    // Create task info
    TaskInfo task;
    task.runnable = runnable;
    task.num_total_tasks = num_total_tasks;
    task.tasks_remaining = num_total_tasks;
    task.is_complete = false;
    
    // Add to task info map
    task_info[new_task_id] = task;
    
    // Update total tasks counter
    total_tasks_remaining += num_total_tasks;
    
    // Check if this task has dependencies
    bool has_pending_deps = false;
    
    if (!deps.empty()) {
        std::set<TaskID> pending_deps;
        
        // Check each dependency
        for (TaskID dep_id : deps) {
            // Skip invalid dependencies
            if (dep_id == 0 || task_info.find(dep_id) == task_info.end()) {
                continue;
            }
            
            // If the dependency is not complete, add it to pending dependencies
            if (!task_info[dep_id].is_complete) {
                pending_deps.insert(dep_id);
                
                // Add this task to the dependent_tasks list of the dependency
                task_info[dep_id].dependent_tasks.insert(new_task_id);
                has_pending_deps = true;
            }
        }
        
        // If there are pending dependencies, add to waiting tasks
        if (has_pending_deps) {
            waiting_tasks[new_task_id] = pending_deps;
        }
    }
    
    // If no pending dependencies, add all tasks to ready queue
    if (!has_pending_deps) {
        for (int i = 0; i < num_total_tasks; i++) {
            ready_queue.push(std::make_tuple(new_task_id, i, num_total_tasks));
        }
        
        // Notify worker threads that new tasks are available
        cv_tasks_available.notify_all();
    }
    
    return new_task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    // Wait until all tasks are completed
    std::unique_lock<std::mutex> lock(queue_mutex);
    cv_tasks_done.wait(lock, [this]() {
        return total_tasks_remaining == 0;
    });
}
