#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <thread>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <queue>
#include <atomic>
#include <map>
#include <set>
#include <unordered_map>
#include <deque>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    private:
        int num_threads;
        std::vector<std::thread> thread_pool;
        
        // Mutex for protecting shared data
        std::mutex queue_mutex;
        
        // Condition variables
        std::condition_variable cv_tasks_available;
        std::condition_variable cv_tasks_done;
        
        // Termination flag
        bool should_terminate;
        
        // Task information
        struct TaskInfo {
            IRunnable* runnable;
            int num_total_tasks;
            int tasks_remaining;
            std::vector<TaskID> dependent_tasks; // Tasks that depend on this one (optimized)
            bool is_complete;
        };
        
        // Task queue for ready tasks - using deque for better cache locality
        // Each entry is (task_id, task_index, total_tasks)
        std::deque<std::tuple<TaskID, int, int>> ready_queue;
        
        // Map to store task information
        std::unordered_map<TaskID, TaskInfo> task_info;
        
        // Tasks waiting on dependencies
        std::unordered_map<TaskID, std::set<TaskID>> waiting_tasks; // task_id -> set of dependencies
        
        // Counter for generating task IDs
        std::atomic<TaskID> next_task_id;
        
        // Count of all tasks still running in the system
        std::atomic<int> total_tasks_remaining;
        
        // Helper methods
        void checkAndEnqueueWaitingTasks(TaskID completed_task_id);
        void bulkEnqueueTasks(TaskID task_id, int num_total_tasks);
        
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

#endif
