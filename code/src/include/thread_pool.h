#ifndef _THREAD_POOL_HPP
#define _THREAD_POOL_HPP
#include <list>
#include <memory>
#include <functional>
#include <type_traits>
#include <thread>
#include <future>
#include <mutex>
#include <condition_variable>

namespace DB::util
{


    // type erasion refers to https://github.com/progschj/ThreadPool
    // as to wrap the packaged_task into a lambda.
    // since F must be `CopyConstructible`, we must use shared_ptr.
    // https://en.cppreference.com/w/cpp/utility/functional/function/function


    class ThreadsPool
    {
    public:
        enum class schedule_policy { FIFO, LIFO };
    public:
        ThreadsPool(schedule_policy policy = schedule_policy::FIFO) :_schedule_policy(policy) {}
        ThreadsPool(const ThreadsPool&) = delete;
        ThreadsPool& operator=(const ThreadsPool&) = delete;
        // return immediately !!!
        template<typename F, typename... Args>[[nodiscard]]
            std::future<std::invoke_result_t<F, Args...>> register_for_execution(F&& f, Args&& ...args)
        {
            using Ret = std::invoke_result_t<F, Args...>;
            std::shared_ptr<std::packaged_task<Ret()>> task =
                std::make_shared<std::packaged_task<Ret()>>(std::bind(std::forward<F>(f), std::forward<Args>(args)...));
            std::future<Ret> fut = task->get_future();
            std::lock_guard<std::mutex> lg{ _mtx };
            _tasks.emplace_back([task]() { (*task)(); });
            return fut;
        }
        void start() { std::call_once(_start_flag, [this]() { std::thread(std::mem_fn(&ThreadsPool::schedule), this).detach(); }); }
        void stop()
        {
            std::call_once(_stop_flag,
                [this]()
            {
                _prom.set_value();
                // wait until all treads has accomplished, since this obj may be desturcted.
                std::unique_lock<std::mutex> ulk{ _mtx };
                while (num_threads != max_threads)
                    _cv.wait(ulk);
            });
        }
        ~ThreadsPool() { stop(); }
    private:
        using task_t = std::function<void()>;
        static constexpr int max_threads = 3;
        int num_threads = max_threads;
        const schedule_policy _schedule_policy;
        std::once_flag _start_flag;
        std::once_flag _stop_flag;
        std::promise<void> _prom;
        std::mutex _mtx;
        std::condition_variable _cv;
        std::list<task_t> _tasks;

        void schedule()
        {
            std::future<void> f = _prom.get_future();
            while (true)
            {
                using namespace std::literals::chrono_literals;
                if (f.wait_for(50ns) == std::future_status::ready)
                    break;
                bool schedule = false;
                {
                    std::lock_guard<std::mutex> lg{ _mtx };
                    if (num_threads > 0 && !_tasks.empty())
                        schedule = true;
                }
                if (schedule) schedule_once();
            }
        } // end function void schedule();

        void schedule_once()
        {
            // consume one thread
            std::unique_lock<std::mutex> ulk{ _mtx };
            while (num_threads == 0)
                _cv.wait(ulk);
            num_threads--;
            task_t task;
            switch (_schedule_policy)
            {
            case schedule_policy::FIFO:
                task = std::move(_tasks.front());
                _tasks.pop_front();
                break;
            case schedule_policy::LIFO:
                task = std::move(_tasks.back());
                _tasks.pop_back();
                break;
            default:
                break;
            }

            ulk.unlock();

            std::thread([this, task{ std::move(task) }]() mutable
            {
                // execute task
                task();
                std::lock_guard<std::mutex> lg{ _mtx };
                num_threads++;
                _cv.notify_one();
            }).detach();
        } // end function void schedule_once();
    }; // end class ThreadsPool



} // end namespace DB::util

#endif // !_THREAD_POOL_HPP