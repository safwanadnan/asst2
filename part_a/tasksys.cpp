#include "tasksys.h"
#include <thread>


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
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
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
    this->num_threads = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // Create worker threads
    std::vector<std::thread> workers;
    workers.reserve(num_threads);
    
    // Worker function for each thread
    auto worker_function = [runnable, num_total_tasks](int start_task, int end_task) {
        for (int i = start_task; i < end_task; i++) {
            runnable->runTask(i, num_total_tasks);
        }
    };
    
    // Calculate tasks per thread
    int tasks_per_thread = (num_total_tasks + num_threads - 1) / num_threads;
    
    // Launch worker threads
    for (int i = 0; i < num_threads; i++) {
        int start_task = i * tasks_per_thread;
        int end_task = std::min((i + 1) * tasks_per_thread, num_total_tasks);
        
        // Don't create empty threads
        if (start_task >= num_total_tasks) {
            break;
        }
        
        workers.push_back(std::thread(worker_function, start_task, end_task));
    }
    
    // Join all worker threads
    for (auto& worker : workers) {
        worker.join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
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
    this->num_threads = num_threads;
    should_terminate = false;
    
    // Create the thread pool
    for (int i = 0; i < num_threads; i++) {
        thread_pool.push_back(std::thread([this]() {
            while (true) {
                // Check for termination
                if (should_terminate) {
                    break;
                }
                
                // Try to get a task from the queue
                IRunnable* runnable = nullptr;
                int task_id = -1;
                int num_total_tasks = 0;
                
                {
                    std::lock_guard<std::mutex> lock(queue_mutex);
                    if (!task_queue.empty()) {
                        auto task = task_queue.front();
                        task_queue.pop();
                        
                        runnable = std::get<0>(task);
                        task_id = std::get<1>(task);
                        num_total_tasks = std::get<2>(task);
                    }
                }
                
                // If we got a task, execute it
                if (runnable != nullptr) {
                    runnable->runTask(task_id, num_total_tasks);
                    
                    // Decrement tasks remaining
                    tasks_remaining--;
                }
                
                // Otherwise continue spinning
            }
        }));
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    // Signal threads to terminate
    should_terminate = true;
    
    // Wake up any threads waiting on the queue
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        // Clear any remaining tasks
        while (!task_queue.empty()) {
            task_queue.pop();
        }
    }
    
    // Join all threads
    for (auto& thread : thread_pool) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // Reset tasks counter
    tasks_remaining = num_total_tasks;
    
    // Add tasks to the queue
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        for (int i = 0; i < num_total_tasks; i++) {
            task_queue.push(std::make_tuple(runnable, i, num_total_tasks));
        }
    }
    
    // Wait for all tasks to complete (spin waiting)
    while (tasks_remaining > 0) {
        // Just spin
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
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
    tasks_remaining = 0;
    
    // Create the thread pool
    for (int i = 0; i < num_threads; i++) {
        thread_pool.push_back(std::thread([this]() {
            while (true) {
                IRunnable* runnable = nullptr;
                int task_id = -1;
                int num_total_tasks = 0;
                
                // Get a task from the queue or wait if none available
                {
                    std::unique_lock<std::mutex> lock(queue_mutex);
                    
                    // Wait until there's a task or the system is terminating
                    cv_tasks_available.wait(lock, [this]() {
                        return !task_queue.empty() || should_terminate;
                    });
                    
                    // If we're terminating, exit the thread
                    if (should_terminate) {
                        break;
                    }
                    
                    // Get the task
                    if (!task_queue.empty()) {
                        auto task = task_queue.front();
                        task_queue.pop();
                        
                        runnable = std::get<0>(task);
                        task_id = std::get<1>(task);
                        num_total_tasks = std::get<2>(task);
                    }
                }
                
                // Execute the task outside the lock
                if (runnable != nullptr) {
                    runnable->runTask(task_id, num_total_tasks);
                    
                    // Decrement tasks remaining count
                    int remaining = --tasks_remaining;
                    
                    // If this was the last task, notify the waiting thread
                    if (remaining == 0) {
                        cv_tasks_done.notify_all();
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
    
    // Notify all waiting threads to check termination flag
    cv_tasks_available.notify_all();
    
    // Join all threads
    for (auto& thread : thread_pool) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    // No tasks to run
    if (num_total_tasks == 0) {
        return;
    }
    
    // Set the tasks remaining counter
    tasks_remaining = num_total_tasks;
    
    // Add all tasks to the queue
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        for (int i = 0; i < num_total_tasks; i++) {
            task_queue.push(std::make_tuple(runnable, i, num_total_tasks));
        }
    }
    
    // Wake up worker threads
    cv_tasks_available.notify_all();
    
    // Wait for tasks to complete
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        cv_tasks_done.wait(lock, [this]() {
            return tasks_remaining == 0;
        });
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
