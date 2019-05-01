#ifndef _VM_H
#define _VM_H
#include "disk_manager.h"
#include "buffer_pool.h"
#include "BplusTree.h"
#include "thread_pool.h"
#include "page.h"
#include "table.h"
#include <future>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <queue>
#include <thread>
#include <condition_variable>
#include <memory>
#include <deque>

namespace DB::tree { class BTree; }

namespace DB::vm
{

    using table::table_view;

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
        void add_sql(std::string); // only used when redo
    private:
        std::queue<std::string> sql_pool_;
        std::thread reader_;
        std::mutex sql_pool_mutex_;
        std::condition_variable sql_pool_cv_;
    };


    using table::value_t;
    using col_name_t = std::string;
    using row_t = page::ValueEntry;

    class row_view
    {
    public:

        row_view(table_view, row_t);

        bool isEOF() const;
        void setEOF();
        value_t getValue(col_name_t colName) const;

    private:
        bool eof_;
        table_view table_view_;
        std::shared_ptr<row_t>  row_;
    };


    // used as a temporary table.
    // implementation might be stream ?? with some sync mechanism.
    class VitrualTable
    {
        struct channel_t {
            std::queue<row_view> row_buffer_;
            std::mutex mtx_;
            std::condition_variable cv_;
        };

    public:

        VitrualTable(table_view);
        VitrualTable(const VitrualTable&) = default;
        VitrualTable& operator=(const VitrualTable&) = default;

        void addRow(row_view row);
        void addEOF();

        void set_size(uint32_t);            // [maybe_unused], since on sigma-node, we can not
        uint32_t size() const;              // decide the size before iterating all rows.

        row_view getRow();                  // might be stuck
        std::deque<row_view> waitAll();     // might be stuck

        table_view table_view_;             // info of table: col, constraint...

    private:

        uint32_t size_;
        std::shared_ptr<channel_t> ch_;

    };


    //
    //
    //
    //
    class VM
    {
        friend class disk::DiskManager;
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

        void set_next_free_page_id(page::page_id_t);

    private:

        void send_reply_sql(std::string);

        void query_process();

        void doWAL(page::page_id_t prev_last_page_id, const std::string& sql);

        // flush dirty page into disk.
        void flush();

        void detroy_log();

        // return   OK   : no need to reply log.
        //          UNDO : undo the last txn.
        //          REDO : undo the last txn, then redo.
        disk::log_state_t check_log();
        void replay_log(disk::log_state_t);

        // 4 kinds of op node
        // - scanTable
        // - join
        // - projection
        // - sigma
        VitrualTable scanTable(const std::string& tableName);
        void doScanTable(VitrualTable ret, const std::string tableName);

        VitrualTable join(VitrualTable t1, VitrualTable t2, bool pk);
        void doJoin(VitrualTable ret, VitrualTable t1, VitrualTable t2, bool pk);

        VitrualTable projection(VitrualTable t);
        void doProjection(VitrualTable ret, VitrualTable t);

        VitrualTable sigma(VitrualTable t /* where-root */);
        void doSigma(VitrualTable ret, VitrualTable t /* where-root */);


        void init_pk_view();

    private:

        StorageEngine storage_engine_;
        util::ThreadsPool task_pool_;
        ConsoleReader conslole_reader_;
        page::DBMetaPage* db_meta_;
        std::unordered_map<std::string, page::TableMetaPage*> table_meta_;

        // PK view: pk -> ref
        // if the PK is not ref by any FK, the ref = 1;
        friend class table::TableInfo;
        std::unordered_map<page::page_id_t,
            std::unordered_map<int32_t, uint32_t>> table_pk_ref_INT;
        std::unordered_map<page::page_id_t,
            std::unordered_map<std::string, uint32_t>> table_pk_ref_VARCHAR;


    public: // for test
        void test_create_table();
        uint32_t test_insert(const tree::KVEntry&);
        uint32_t test_erase(const page::KeyEntry&);
        page::ValueEntry test_find(const page::KeyEntry&);
        void test_size();
        void test_output();
        void test_flush();

    };



} // end namespace DB::vm

#endif // !_VM_H