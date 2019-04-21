#ifndef _VM_H
#define _VM_H
#include "disk_manager.h"
#include "buffer_pool.h"
#include "BplusTree.h"
#include "thread_pool.h"
#include "page.h"
#include <future>
#include <unordered_map>
#include <mutex>
#include <queue>
#include <thread>
#include <condition_variable>

namespace DB::tree { class BTree; }

namespace DB::vm
{

    struct StorageEngine
    {
        disk::DiskManager* disk_manager_;
        buffer::BufferPoolManager* buffer_pool_manager_;
    };


    class ConsoleReader
    {
        ConsoleReader(const ConsoleReader&) = delete;
        ConsoleReader(ConsoleReader&&) = delete;
        ConsoleReader& operator=(const ConsoleReader&) = delete;
        ConsoleReader& operator=(ConsoleReader&&) = delete;
    public:
        ConsoleReader() = default;
        void start();
        void stop();
        std::string get_sql(); // might stuck
    private:
        std::queue<std::string> sql_pool_;
        std::thread reader_;
        std::mutex sql_pool_mutex_;
        std::condition_variable sql_pool_cv_;
    };


    class Table
    {

    };


    // used as a temporary table.
    // implementation might be stream ?? with some sync mechanism.
    class VitrualTable {


    };


    //
    //
    //
    //
    class VM
    {
    public:

        VM();
        ~VM();

        VM(const VM&) = delete;
        VM(VM&&) = delete;
        VM& operator=(const VM&) = delete;
        VM& operator=(VM&&) = delete;

        void init();

        // run db task until user input "exit"
        void start();

        template<typename F, typename... Args>[[nodiscard]]
            std::future<std::invoke_result_t<F, Args...>> register_task(F&& f, Args&& ...args) {
            return task_pool_.register_for_execution(f, args...);
        }

    private:

        void query_process();

        void doWAL();

        // flush dirty page into disk.
        void flush();

        void detroy_log();

        // return   OK   : no need to reply log.
        //          UNDO : undo the last txn.
        //          REDO : undo the last txn, then redo.
        disk::log_state_t check_log();
        void replay_log(disk::log_state_t);


    private:

        StorageEngine storage_engine_;
        util::ThreadsPool task_pool_;
        ConsoleReader conslole_reader_;
        page::DBMetaPage* db_meta_;
        std::unordered_map<std::string, page::TableMetaPage*> table_meta_;


    public: // for test
        void test_create_table();
        uint32_t test_insert(const tree::KVEntry&);
        uint32_t test_earse(const page::KeyEntry&);
        page::ValueEntry test_find(const page::KeyEntry&);
        void test_output();
        void test_flush();

    };



} // end namespace DB::vm

#endif // !_VM_H