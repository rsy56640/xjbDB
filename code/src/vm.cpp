#include "include/vm.h"

namespace DB::vm
{

    using namespace page;
    using disk::log_state_t;

    void ConsoleReader::start()
    {
        // TODO: read from console, and put into sql pool, then notify.

    }

    void ConsoleReader::stop() {
        if (reader_.joinable())
            reader_.join();
    }

    std::string ConsoleReader::get_sql()
    {
        std::unique_lock<std::mutex> ulk{ sql_pool_mutex_ };
        while (sql_pool_.empty())
            sql_pool_cv_.wait(ulk);
        std::string sql = std::move(sql_pool_.front());
        sql_pool_.pop();
        return sql;
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
            // recover from log
            log_state_t log_state = check_log();
            if (log_state != log_state_t::OK)
                replay_log(log_state);

            // read DB meta
            char buffer[page::PAGE_SIZE];
            storage_engine_.disk_manager_->ReadPage(page::NOT_A_PAGE, buffer);
            db_meta_ = page::parse_DBMetaPage(storage_engine_.buffer_pool_manager_, buffer);

            // set vm
            storage_engine_.disk_manager_->set_vm(this);

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
        }

        // start scan from console.
        conslole_reader_.start();
    }


    // run db task until user input "exit"
    void VM::start()
    {
        // TODO: VM::start()


    }



    void VM::set_next_free_page_id(page::page_id_t next_free_page) {
        db_meta_->next_free_page_id_ = next_free_page;
        db_meta_->set_dirty();
    }


    void VM::flush() {
        storage_engine_.buffer_pool_manager_->flush();
    }





    log_state_t VM::check_log() {
        return storage_engine_.disk_manager_->check_log();
    }


    void VM::replay_log(log_state_t log_state)
    {
        // TODO VM::replay_log();

    }


    VM::~VM()
    {
        conslole_reader_.stop();
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
        it.destroy();
        end.destroy();
        table->bt_->range_query_end_unlock();
        std::printf("output size = %d\n", cnt);
    }

    void VM::test_flush() {
        flush();
    }


} // end namespace DB::vm