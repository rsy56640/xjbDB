#include "include/vm.h"
#include "include/debug_log.h"
#include <iostream>
#include <functional>

namespace DB::vm
{

    using namespace page;
    using disk::log_state_t;

    void ConsoleReader::start()
    {
        reader_ = std::thread([this]()
        {
            auto check = [](const std::string& statement)->int32_t {
                const uint32_t size = statement.size();
                for (uint32_t i = 0; i < size; i++)
                    if (statement[i] == ';')
                        return i;
                return -1;
            };
            // check if "  EXIT "
            auto check_exit = [](const std::string& sql)->bool {
                if (sql.empty()) return false;
                uint32_t first = 0;
                uint32_t last = sql.size() - 1;
                for (; first < last && sql[first] == ' '; first++);
                for (; last > first && sql[last] == ' '; last--);
                if (last != first + 3) return false;
                return sql.substr(first, 4) == "EXIT";
            };

            std::string sql = "";
            std::string statement = "";
            while (true) {
                std::cin >> statement;
                // meet ';'
                bool meet = false;
                int32_t semicolon;
                if (semicolon = check(statement) != -1)
                {
                    sql += statement.substr(0, semicolon);
                    bool exit = check_exit(sql);
                    std::lock_guard<std::mutex> lg{ sql_pool_mutex_ };
                    sql_pool_.push(std::move(sql));
                    sql_pool_cv_.notify_one();
                    meet = true;

                    if (exit)
                        return;
                }
                else {
                    sql += statement;
                }

                if (meet)
                    sql = statement.substr(semicolon + 1);

                statement = "";

            } // end while-loop
        }); // reader thread
    }

    void ConsoleReader::stop() {
        if (reader_.joinable()) {
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




    row_view::row_view(table_view table_view, row_t row)
        :eof_(false), table_view_(table_view), row_(std::make_shared<row_t>(row)) {}

    bool row_view::isEOF() const { return eof_; }

    void row_view::setEOF() { eof_ = true; }

    //value_t row_view::getValue(col_name_t colName) const {
    //}




    VitrualTable::VitrualTable(table_view table_view)
        :table_view_(table_view), ch_(std::make_shared<channel_t>()) {}

    void VitrualTable::addRow(row_view row) {
        std::lock_guard<std::mutex> lg{ ch_->mtx_ };
        ch_->row_buffer_.push(row);
        ch_->cv_.notify_one();
    }

    void VitrualTable::addEOF() {
        row_t null_row;
        row_view eof_row(table_view_, null_row);
        eof_row.setEOF();
        std::lock_guard<std::mutex> lg{ ch_->mtx_ };
        ch_->row_buffer_.push(eof_row);
        ch_->cv_.notify_one();
    }

    void VitrualTable::set_size(uint32_t size) { size_ = size; }

    uint32_t VitrualTable::size() const { return size_; }

    row_view VitrualTable::getRow() {
        std::unique_lock<std::mutex> ulk{ ch_->mtx_ };
        ch_->cv_.wait(ulk, [this]() { return !ch_->row_buffer_.empty(); });
        row_view row = ch_->row_buffer_.front();
        ch_->row_buffer_.pop();
        return row;
    }

    std::deque<row_view> VitrualTable::waitAll() {
        std::deque<row_view> ret_table;
        std::unique_lock<std::mutex> ulk{ ch_->mtx_ };
        while (ret_table.empty() || !ret_table.back().isEOF()) {
            ch_->cv_.wait(ulk, [this]() { return !ch_->row_buffer_.empty(); });
            while (!ch_->row_buffer_.empty()) {
                ret_table.push_back(ch_->row_buffer_.front());
                ch_->row_buffer_.pop();
            }
        }
        return ret_table;
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

            init_pk_view(); // cache pk in memory, for FK constraint use

        } // end rebuild DB

        // start task pool
        task_pool_.start();

#ifndef _xjbDB_test_STORAGE_ENGINE_
        // start scan from console.
        conslole_reader_.start();
#endif // !_xjbDB_test_STORAGE_ENGINE_
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
            // reader.stop();


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



        // TODO: update PK view

    }


    void VM::doWAL(page::page_id_t prev_last_page_id, const std::string& sql) {
        storage_engine_.disk_manager_->doWAL(prev_last_page_id, sql);
    }

    void VM::flush() {
        db_meta_->flush();
        for (auto&[name, table] : table_meta_)
            table->flush();
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


    //
    // 4 op node service providing for sql logic
    //

    VitrualTable VM::scanTable(const std::string& tableName) {
        table_view tv; // TODO: table_view for scanTable
        VitrualTable vt(tv);
        std::future<void> no_use =
            register_task(std::mem_fn(&VM::doScanTable), this, vt, tableName);
        return vt;
    }

    void VM::doScanTable(VitrualTable ret, const std::string tableName) {
        using namespace tree;
        auto table = table_meta_.find(tableName);
        if (table == table_meta_.end()) {
            debug::ERROR_LOG("scan non-existing table.\n");
            return;
        }
        BTree* bt = table->second->bt_;
        bt->range_query_begin_lock();
        tree::BTit it = bt->range_query_from_begin();
        tree::BTit end = bt->range_query_from_end();
        while (it != end) {
            ret.addRow({ ret.table_view_, it.getV() });
            ++it;
        }
        bt->range_query_end_unlock();
        ret.addEOF();
    }


















    void VM::init_pk_view() {
        struct FKInfo {
            page::key_t_t key_t;
            std::unordered_map<int32_t, uint32_t>* pk_int = nullptr;
            std::unordered_map<std::string, uint32_t>* pk_str = nullptr;
            range_t range;
        };

        for (auto&[_, table] : table_meta_)
        {

            if (!table->hasPK()) continue;

            if (table->PK_t() == key_t_t::INTEGER)
            {
                std::unordered_map<int32_t, uint32_t>& pk_view
                    = table_pk_ref_INT[table->get_page_id()];
                tree::BTree* bt = table->bt_;

                std::vector<FKInfo> fks;
                for (auto const&[name, col] : table->col_name2col_) {
                    if (col->isFK()) {
                        FKInfo info;
                        info.key_t = col->col_t_;
                        if (info.key_t == key_t_t::INTEGER)
                            info.pk_int = &table_pk_ref_INT[col->other_value_];
                        else
                            info.pk_str = &table_pk_ref_VARCHAR[col->other_value_];
                        info.range = table->get_col_range(name);
                        fks.push_back(info);
                    }
                }

                tree::BTit it = bt->range_query_from_begin();
                tree::BTit end = bt->range_query_from_end();
                while (it != end) {
                    KeyEntry kEntry = it.getK();
                    pk_view[kEntry.key_int]++;

                    for (FKInfo const& fk : fks) {
                        const ValueEntry vEntry = it.getV();
                        if (fk.key_t == key_t_t::INTEGER) {
                            (*fk.pk_int)[get_range_INT(vEntry, fk.range)]++;
                        }
                        else {
                            (*fk.pk_str)[get_range_VARCHAR(vEntry, fk.range)]++;
                        }
                    }

                    ++it;
                } // end iterate bt

            } // end table pk is INTEGER

            else // table pk is CHAR / VARCHAR
            {
                std::unordered_map<std::string, uint32_t>& pk_view
                    = table_pk_ref_VARCHAR[table->get_page_id()];
                tree::BTree* bt = table->bt_;

                std::vector<FKInfo> fks;
                for (auto const&[name, col] : table->col_name2col_) {
                    if (col->isFK()) {
                        FKInfo info;
                        info.key_t = col->col_t_;
                        if (info.key_t == key_t_t::INTEGER)
                            info.pk_int = &table_pk_ref_INT[col->other_value_];
                        else
                            info.pk_str = &table_pk_ref_VARCHAR[col->other_value_];
                        info.range = table->get_col_range(name);
                        fks.push_back(info);
                    }
                }

                tree::BTit it = bt->range_query_from_begin();
                tree::BTit end = bt->range_query_from_end();
                while (it != end) {
                    KeyEntry kEntry = it.getK();
                    pk_view[kEntry.key_str]++;

                    for (FKInfo const& fk : fks) {
                        const ValueEntry vEntry = it.getV();
                        if (fk.key_t == key_t_t::INTEGER) {
                            (*fk.pk_int)[get_range_INT(vEntry, fk.range)]++;
                        }
                        else {
                            (*fk.pk_str)[get_range_VARCHAR(vEntry, fk.range)]++;
                        }
                    }

                    ++it;
                } // end iterate bt
            
            } // end table pk is CHAR / VARCHAR

        } // end iterate all table

    } // end function `void VM::init_pk_view();`


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
        pkCol->vEntry_offset = 0;
        table->insert_column("tid", pkCol);

        ColumnInfo* col = new ColumnInfo;
        col->col_t_ = col_t_t::VARCHAR;
        col->str_len_ = 20;
        col->vEntry_offset = sizeof(int32_t);
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