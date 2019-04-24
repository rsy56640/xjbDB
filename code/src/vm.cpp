#include "include/vm.h"
#include "include/debug_log.h"
#include <iostream>
#include <chrono>

namespace DB::vm
{

    using namespace page;
    using disk::log_state_t;

    void ConsoleReader::start()
    {
        reader_ = std::thread([this]() {
            auto check = [](const std::string& statement)->int32_t {
                const uint32_t size = statement.size();
                for (uint32_t i = 0; i < size; i++)
                    if (statement[i] == ';')
                        return i;
                return -1;
            };

            using namespace std::chrono_literals;
            std::future stop = stop_flag_.get_future();

            std::string sql = "";
            std::string statement = "";
            while (true) {

                if (stop.wait_for(50ns) == std::future_status::ready)
                    return;

                std::cin >> statement;
                // meet ';'
                bool meet = false;
                int32_t semicolon;
                if (semicolon = check(statement) != -1) {
                    sql += statement.substr(0, semicolon);
                    std::lock_guard<std::mutex> lg{ sql_pool_mutex_ };
                    sql_pool_.push(std::move(sql));
                    sql_pool_cv_.notify_one();
                    meet = true;
                }
                else {
                    sql += statement;
                }
                if (meet)
                    sql = statement.substr(semicolon + 1);
                statement = "";
            }
        });
    }

    void ConsoleReader::stop() {
        if (reader_.joinable()) {
            stop_flag_.set_value();
            reader_.join();
        }
    }

    std::string ConsoleReader::get_sql() {
        std::unique_lock<std::mutex> ulk{ sql_pool_mutex_ };
        while (sql_pool_.empty())
            sql_pool_cv_.wait(ulk);
        std::string sql = std::move(sql_pool_.front());
        sql_pool_.pop();
        return sql;
    }

    void ConsoleReader::add_sql(std::string sql) {
        std::lock_guard<std::mutex> lg{ sql_pool_mutex_ };
        sql_pool_.push(std::move(sql));
    }


    VM::VM()
    {
        storage_engine_.disk_manager_ = new disk::DiskManager{};
        storage_engine_.buffer_pool_manager_ = new buffer::BufferPoolManager{ storage_engine_.disk_manager_ };
    }


    void VM::init()
    {
        // init DB
        if (storage_engine_.disk_manager_->dn_init_)
        {
            db_meta_ = new DBMetaPage(NOT_A_PAGE, // DB meta
                storage_engine_.disk_manager_, true, 0, 0, NOT_A_PAGE);
            storage_engine_.disk_manager_->set_vm(this);
        }

        // rebuild DB
        else
        {
            // set vm
            storage_engine_.disk_manager_->set_vm(this);

            // check log for undo and redo
            const log_state_t log_state = check_log();
            switch (log_state)
            {
            case log_state_t::CORRUPTION: // exit directly
                debug::ERROR_LOG("log corruption, bomb, you die\n");
                exit(1);
            case log_state_t::REDO:
            case log_state_t::UNDO:
                replay_log(log_state);
            case log_state_t::OK:
            default:
                break;
            }

            // read DB meta
            char buffer[page::PAGE_SIZE];
            storage_engine_.disk_manager_->ReadPage(page::NOT_A_PAGE, buffer);
            db_meta_ = page::parse_DBMetaPage(storage_engine_.buffer_pool_manager_, buffer);

            // set cur_page_no
            storage_engine_.disk_manager_->set_cur_page_id(db_meta_->cur_page_no_);

            // set next_free_page_id
            storage_engine_.disk_manager_->init_set_next_free_page_id(db_meta_->next_free_page_id_);

            // read table meta
            for (auto const&[tableName, page_id] : db_meta_->table_name2id_)
            {
                storage_engine_.disk_manager_->ReadPage(page_id, buffer);
                TableMetaPage* table_meta = parse_TableMetaPage(storage_engine_.buffer_pool_manager_, buffer);
                table_meta_[tableName] = table_meta;
            }

        } // end rebuild DB

        // start task pool
        task_pool_.start();

        // start scan from console.
        conslole_reader_.start();

    }


    // run db task until user input "EXIT"
    void VM::start()
    {
        while (true)
        {
            const std::string sql_statemt = conslole_reader_.get_sql();

            // TODO: get query plan


            // handle ErrorMsg or EXIT

            // EXIT
            // stop scan from console and process all current sql
            // conslole_reader_.stop();

            // UNDONE: if crash, how to find `prev_last_page_id` when rebuild,
            //         since DBMetaPage only writes `cur_page_id` after `query_process();`
            const page::page_id_t prev_last_page_id =
                storage_engine_.disk_manager_->get_cut_page_id();

            query_process();

            task_pool_.join();

            doWAL(prev_last_page_id, sql_statemt);

            flush();

            detroy_log();
        }
    }



    void VM::set_next_free_page_id(page::page_id_t next_free_page) {
        db_meta_->next_free_page_id_ = next_free_page;
        db_meta_->set_dirty();
    }



    void VM::send_reply_sql(std::string sql) {
        conslole_reader_.add_sql(std::move(sql));
    }


    void VM::query_process()
    {
        // TODO: query process


    }


    void VM::doWAL(page::page_id_t prev_last_page_id, const std::string& sql) {
        storage_engine_.disk_manager_->doWAL(prev_last_page_id, sql);
    }

    void VM::flush() {
        storage_engine_.buffer_pool_manager_->flush();
    }


    void VM::detroy_log() {
        storage_engine_.disk_manager_->detroy_log();
    }


    log_state_t VM::check_log() {
        return storage_engine_.disk_manager_->check_log_state();
    }


    void VM::replay_log(log_state_t log_state) {
        storage_engine_.disk_manager_->replay_log(log_state);
    }


    VM::~VM()
    {
        conslole_reader_.stop();
        task_pool_.stop();
        delete db_meta_;
        for (auto&[k, v] : table_meta_)
            delete v;
        delete storage_engine_.buffer_pool_manager_;
        delete storage_engine_.disk_manager_;
    }


    /////////////////////////////////// test function ///////////////////////////////////

    void VM::test_create_table()
    {
        TableMetaPage* table = new TableMetaPage(storage_engine_.buffer_pool_manager_,
            storage_engine_.disk_manager_->AllocatePage(), storage_engine_.disk_manager_,
            true, key_t_t::INTEGER, 0, NOT_A_PAGE, 0, NOT_A_PAGE);
        table_meta_["test"] = table;

        ColumnInfo* pkCol = new ColumnInfo;
        pkCol->col_t_ = col_t_t::INTEGER;
        pkCol->setPK();
        table->insert_column("tid", pkCol);

        ColumnInfo* col = new ColumnInfo;
        col->col_t_ = col_t_t::VARCHAR;
        col->str_len_ = 20;
        table->insert_column("name", col);

        db_meta_->insert_table(table->get_page_id(), "test");
    }

    uint32_t VM::test_insert(const tree::KVEntry& kvEntry) {
        TableMetaPage* table = table_meta_["test"];
        return table->bt_->insert(kvEntry);
    }

    uint32_t VM::test_erase(const page::KeyEntry& kEntry) {
        TableMetaPage* table = table_meta_["test"];
        return table->bt_->erase(kEntry);
    }

    page::ValueEntry VM::test_find(const page::KeyEntry& kEntry) {
        TableMetaPage* table = table_meta_["test"];
        return table->bt_->find(kEntry);
    }

    void VM::test_size() {
        TableMetaPage* table = table_meta_["test"];
        std::printf("size = %d\n", table->bt_->size());
    }

    void VM::test_output() {
        TableMetaPage* table = table_meta_["test"];
        int cnt = 0;
        table->bt_->range_query_begin_lock();
        auto it = table->bt_->range_query_from_begin();
        auto end = table->bt_->range_query_from_end();
        while (it != end) {
            std::printf("%d -> %s\n", it.getK().key_int, it.getV().content_);
            ++it;
            cnt++;
        }
        table->bt_->range_query_end_unlock();
        std::printf("output size = %d\n", cnt);
    }

    void VM::test_flush() {
        flush();
    }


} // end namespace DB::vm