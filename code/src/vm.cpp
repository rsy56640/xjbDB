#include "include/vm.h"
#include "include/debug_log.h"
#include "include/table.h"
#include "ast_tp.h"
#include "query_tp.h"
#include <iostream>
#include <variant>
#include <functional>
#include <vector>
#include <deque>
#include <tuple>

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
            // refer to issue #2 https://github.com/rsy56640/xjbDB/issues/2
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
                char buffer[256];
                printXJBDB("");
                std::cin.getline(buffer, 256);
                statement = buffer;
                // meet ';'
                bool meet = false;
                int32_t semicolon;
                if ((semicolon = check(statement)) != -1)
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
                    sql += statement + " ";
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

            for (auto const&[name, _] : table_meta_)
                table_info_[name] = getTableInfo(name).value();

            init_pk_view(); // cache pk in memory, for FK constraint use

        } // end rebuild DB

        // send vm handler to ast for op nodes service
        table::vm_ = this;

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

            query::TPValue plan = query::tp_parse(sql_statemt);

            const page::page_id_t prev_last_page_id =
                storage_engine_.disk_manager_->get_cut_page_id();

            // handle ErrorMsg or EXIT
            VM::process_result_t result = query_process(plan);

            if (!result.msg.empty())
                printXJBDB("\n%s\n", result.msg.c_str());
            if (result.exit)
                return;
            if (result.error)
                continue;

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


    std::optional<table::TableInfo> VM::getTableInfo(const std::string& tableName) {
        auto it = table_meta_.find(tableName);
        if (it == table_meta_.end())
            return std::nullopt;
        const page::TableMetaPage* table_page = it->second;
        std::vector<std::string> colNames = table_page->cols_;
        std::vector<page::ColumnInfo> columnInfos;
        columnInfos.reserve(colNames.size());
        for (const auto& col_name : colNames)
            columnInfos.push_back(*(table_page->col_name2col_.find(col_name)->second));
        table::TableInfo tableInfo;
        tableInfo.tableName_ = tableName;
        tableInfo.colNames_ = std::move(colNames);
        tableInfo.columnInfos_ = std::move(columnInfos);
        tableInfo.reset(this);
        return tableInfo;
    }


    void VM::send_reply_sql(std::string sql) {
        conslole_reader_.add_sql(std::move(sql));
    }


    template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
    template<class... Ts> overloaded(Ts...)->overloaded<Ts...>;
    VM::process_result_t VM::query_process(const query::TPValue& plan)
    {
        VM::process_result_t result;
        std::visit(
            overloaded{
                [&result, this](const query::CreateTableInfo& info) { doCreate(result,info); },
                [&result, this](const query::DropTableInfo& info) { doDrop(result,info); },
                [&result, this](const query::TPSelectInfo& info) { doSelect(result, info);  },
                [&result, this](const query::UpdateInfo& info) { doUpdate(result,info); },
                [&result, this](const query::InsertInfo& info) { doInsert(result,info); },
                [&result, this](const query::DeleteInfo& info) { doDelete(result,info); },
                [&result](query::Exit) { result.exit = true; result.msg = "DB exit"; },
                [&result, this](query::Show) { showDB(); },
                [&result](query::ErrorMsg) { result.error = true; result.msg = "SQL syntax error, please check query log"; },
                [](auto&&) { debug::ERROR_LOG("`query_process`\n"); },
            }, plan);
        return result;
    }


    void VM::doWAL(page::page_id_t prev_last_page_id, const std::string& sql) {
        storage_engine_.disk_manager_->doWAL(prev_last_page_id, sql);
    }

    void VM::flush()
    {
        db_meta_->flush();

        for (auto&[name, table] : table_meta_)
            table->flush();

        for (auto&[name, table] : free_table_)
            delete table;
        free_table_.clear();

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
    // query process functions
    //

    void VM::doCreate(process_result_t& result, const query::CreateTableInfo& info)
    {
        if (table_meta_.size() >= DBMetaPage::MAX_TABLE_NUM) {
            result.error = true;
            result.msg = "DB is full";
            return;
        }
        if (table_meta_.count(info.tableInfo.tableName_)) {
            result.error = true;
            result.msg = "The table \"" + info.tableInfo.tableName_ + "\" has already existed";
            return;
        }

        table::TableInfo tableInfo = info.tableInfo;
        const page_id_t table_id = storage_engine_.disk_manager_->AllocatePage();
        page::TableMetaPage* table_page = new TableMetaPage(
            storage_engine_.buffer_pool_manager_, table_id, storage_engine_.disk_manager_,
            true, tableInfo.PK_t(), tableInfo.str_len(),
            NOT_A_PAGE, 0, NOT_A_PAGE);

        auto default_it = info.defaults.begin();
        auto fk_it = info.fkTables.begin();
        const uint32_t col_size = tableInfo.colNames_.size();
        uint32_t vEntry_offset = 0;

        // AUTOPK
        if (tableInfo.pk_col_ == page::TableMetaPage::NOT_A_COLUMN) {
            page::ColumnInfo* col = new ColumnInfo{};
            col->col_t_ = col_t_t::INTEGER;
            col->setPK();
            col->str_len_ = sizeof(uint32_t);

            col->vEntry_offset_ = vEntry_offset;
            vEntry_offset += col->str_len_;

            table_page->insert_column(page::autoPK, col);
            table_page->pk_col_ = page::TableMetaPage::NOT_A_COLUMN;

            // update tableInfo
            tableInfo.colNames_.insert(tableInfo.colNames_.begin(), page::autoPK);
            tableInfo.columnInfos_.insert(tableInfo.columnInfos_.begin(), *col);
            tableInfo.pk_col_ = page::TableMetaPage::NOT_A_COLUMN;
        }

        table_page->pk_col_ = tableInfo.pk_col_;

        for (uint32_t i = 0; i < col_size; i++)
        {
            page::ColumnInfo* col = new ColumnInfo(tableInfo.columnInfos_[i]);

            col->vEntry_offset_ = vEntry_offset;
            vEntry_offset += col->str_len_;

            if (col->isDEFAULT()) {
                if (col->col_t_ == col_t_t::INTEGER) {
                    int32_t i = std::get<int32_t>(*default_it);
                    table_page->insert_default_value(col, i);
                }
                else {
                    table_page->insert_default_value(col, std::get<std::string>(*default_it));
                }
                ++default_it;
            }

            if (col->isFK()) {
                col->other_value_ = table_meta_[*fk_it]->get_page_id();
                ++fk_it;
            }

            table_page->insert_column(tableInfo.colNames_[i], col);

            // update tableInfo
            if (tableInfo.pk_col_ == page::TableMetaPage::NOT_A_COLUMN)
                tableInfo.columnInfos_[i + 1] = *col;
            else
                tableInfo.columnInfos_[i] = *col;
        }

        db_meta_->insert_table(table_id, tableInfo.tableName_);
        table_meta_[tableInfo.tableName_] = table_page;
        table_info_[tableInfo.tableName_] = tableInfo;

        result.msg = "create table \"" + tableInfo.tableName_ + "\"";
    }

    void VM::doDrop(process_result_t& result, const query::DropTableInfo& info)
    {
        auto it = table_meta_.find(info.tableName);
        if (it == table_meta_.end()) {
            result.error = true;
            result.msg = "the table \"" + info.tableName + "\" does not exist";
            return;
        }

        page::TableMetaPage* table = it->second;

        // check FK constraint
        {
            bool ok_to_drop = true;
            if (table->bt_->key_t() == key_t_t::INTEGER) {
                const std::unordered_map<int32_t, uint32_t >& pk_ref
                    = table_pk_ref_INT[table->get_page_id()];
                for (auto const&[k, ref] : pk_ref)
                    if (ref != NON_FK_REF) {
                        ok_to_drop = false;
                        break;
                    }
            }
            else {
                const std::unordered_map<std::string, uint32_t >& pk_ref
                    = table_pk_ref_VARCHAR[table->get_page_id()];
                for (auto const&[k, ref] : pk_ref)
                    if (ref != NON_FK_REF) {
                        ok_to_drop = false;
                        break;
                    }
            }
            if (!ok_to_drop) {
                result.msg = "table \"" + info.tableName + "\" cannot be dropped due to FK constraint";
                return;
            }
        } // end check FK constraint

        // unref PK view for fk cols
        {
            struct fk_info_t {
                col_t_t col_t;
                page_id_t fk_table;
                range_t range;
            };
            std::vector<fk_info_t> fk_cols;
            for (auto const&[name, col] : table->col_name2col_) {
                if (col->isFK()) {
                    fk_cols.push_back(fk_info_t{
                        col->col_t_,
                        col->other_value_,
                        range_t{ col->vEntry_offset_, col->str_len_ }
                        });
                }
            }

            table->bt_->range_query_begin_lock();
            tree::BTit it = table->bt_->range_query_from_begin();
            tree::BTit end = table->bt_->range_query_from_end();
            while (it != end) {
                const ValueEntry vEntry = it.getV();
                for (auto const& fk_info : fk_cols) {
                    if (fk_info.col_t == col_t_t::INTEGER) {
                        int i = get_range_INT(vEntry, fk_info.range);
                        table_pk_ref_INT[fk_info.fk_table][i]--;
                    }
                    else {
                        std::string s = get_range_VARCHAR(vEntry, fk_info.range);
                        table_pk_ref_VARCHAR[fk_info.fk_table][s]--;
                    }
                }
                ++it;
            }
            table->bt_->range_query_end_unlock();
        } // end unref PK view for fk cols

        db_meta_->drop_table(info.tableName);
        table->bt_->destruct();
        table->set_free(); // also set default_value_page free
        free_table_[info.tableName] = table; // will flush later for WAL
        table_meta_.erase(info.tableName);
        table_info_.erase(info.tableName);

        if (table->PK_t() == key_t_t::INTEGER) {
            table_pk_ref_INT.erase(table->get_page_id());
        }
        else {
            table_pk_ref_VARCHAR.erase(table->get_page_id());
        }

        result.msg = "table \"" + info.tableName + "\" has been dropped";
    }

    void VM::doSelect(process_result_t& result, const query::TPSelectInfo& info)
    {
        // now ignore `ORDER BY`

        VirtualTable result_table = info.opRoot->getOutput();

        // print VirtualTable
        std::shared_ptr<const table::TableInfo> tableInfo = result_table.table_view_.table_info_;
        query_print("table \"%s\"", tableInfo->tableName_.c_str());
        query_print_n();

        // prepare colInfo
        struct output_t {
            col_t_t col_t;
            range_t range;
        };
        std::vector<output_t> outputCol;
        const uint32_t col_size = tableInfo->colNames_.size();
        outputCol.reserve(col_size);
        for (uint32_t i = 0; i < col_size; i++) {
            const page::ColumnInfo& col = tableInfo->columnInfos_[i];
            query_print("%s\t", tableInfo->colNames_[i].c_str());
            outputCol.push_back(output_t{
                col.col_t_, range_t{ col.vEntry_offset_, col.str_len_ }
                });
        }
        query_print_n();
        println();

        uint32_t cnt = 0;
        row_view rv = result_table.getRow();
        while (!rv.isEOF())
        {
            cnt++;
            const ValueEntry& vEntry = *rv.row_;
            for (output_t output : outputCol) {
                if (output.col_t == col_t_t::INTEGER) {
                    query_print("%d\t", get_range_INT(vEntry, output.range));
                }
                else {
                    std::string s = get_range_VARCHAR(vEntry, output.range);
                    query_print("%s\t", s.c_str());
                }
            }
            query_print_n();
            rv = result_table.getRow();
        }
        println();
        query_print("output size = %d", cnt);
        query_print_n();
        println();
    }

    void VM::doUpdate(process_result_t& result, const query::UpdateInfo& info)
    {
        page::TableMetaPage* table = table_meta_[info.sourceTable];
        tree::BTree* bt = table->bt_;
        tree::BTit it = bt->range_query_from_begin();
        tree::BTit end = bt->range_query_from_end();
        uint32_t updated_row_num = 0;

        struct target_t {
            page::range_t range;
            page::col_t_t col_t;
            bool fk;
            page::page_id_t fk_table;
        };
        std::vector<target_t> targets;
        const uint32_t target_size = info.elements.size();
        targets.reserve(target_size);
        for (const query::Element& e : info.elements) {
            ColumnInfo * col = table->col_name2col_[e.name];
            targets.push_back(
                target_t{ page::range_t{ col->vEntry_offset_ ,col->str_len_ },
                col->col_t_ , col->isFK(), col->other_value_ });
        }

        const table_view tv(table_info_[info.sourceTable]);
        while (it != end)
        {
            ValueEntry vEntry = it.getV();
            row_view row(tv, vEntry);
            // check where clause
            if (ast::vmVisit(info.whereExpr, row))
            {
                // "SET name = name +"asd", value = value / 2"
                // update cannot modify PK !!!
                bool ok_to_update = true;
                uint32_t diff_num = 0;
                // maybe someone break FK constraint, so we use a measure like 2PC
                std::vector<uint32_t*> add_ref;
                std::vector<uint32_t*> sub_ref;

                for (uint32_t k = 0; k < target_size; k++)
                {
                    if (!ok_to_update)
                        break;

                    table::value_t v = ast::vmVisitAtom(info.elements[k].valueExpr, row);

                    if (targets[k].col_t == col_t_t::INTEGER)
                    {
                        int32_t i = std::get<int32_t>(v);
                        int32_t old_i = page::get_range_INT(vEntry, targets[k].range);
                        if (i != old_i)
                            diff_num++;
                        // FK constraint
                        if (targets[k].fk) {
                            if (!table_pk_ref_INT[targets[k].fk_table].count(i))
                                ok_to_update = false;
                            else {
                                add_ref.push_back(&table_pk_ref_INT[targets[k].fk_table][i]);
                                sub_ref.push_back(&table_pk_ref_INT[targets[k].fk_table][old_i]);
                            }
                        }
                        if (ok_to_update)
                            page::update_vEntry(vEntry, targets[k].range, i);
                    } // end col is INTEGER
                    else
                    {
                        std::string s = std::get<std::string>(v);
                        std::string old_s = page::get_range_VARCHAR(vEntry, targets[k].range);
                        if (s != old_s)
                            diff_num++;
                        // FK constraint
                        if (targets[k].fk) {
                            if (!table_pk_ref_VARCHAR[targets[k].fk_table].count(s))
                                ok_to_update = false;
                            else {
                                add_ref.push_back(&table_pk_ref_VARCHAR[targets[k].fk_table][s]);
                                sub_ref.push_back(&table_pk_ref_VARCHAR[targets[k].fk_table][old_s]);
                            }
                        }
                        if (ok_to_update)
                            page::update_vEntry(vEntry, targets[k].range, s);
                    } // end col is VARCHAR

                } // end maybe update all column

                // finally commit
                if (ok_to_update) {
                    if (diff_num > 0) {
                        it.updateV(vEntry);
                        updated_row_num++;
                        for (uint32_t* pi : add_ref)
                            *pi = *pi + 1;
                        for (uint32_t* pi : sub_ref)
                            *pi = *pi - 1;
                    }
                }

            } // end whereExpr
            ++it;
        }
        result.msg = "update " + std::to_string(updated_row_num) +
            " rows in table \"" + info.sourceTable + "\"";
    }

    void VM::doInsert(process_result_t& result, const query::InsertInfo& info)
    {
        page::TableMetaPage* table = table_meta_[info.sourceTable];
        tree::BTree* bt = table->bt_;

        // prepare insert values
        struct insert_element {
            page::range_t range;
            table::value_t value;
            page::col_t_t col_t;
            bool fk;
            page::page_id_t fk_table;
        };
        std::vector<insert_element> elements;
        const uint32_t insert_col_size = info.elements.size();
        uint32_t pk_col = TableMetaPage::NOT_A_COLUMN;
        table::value_t pk_v;
        elements.reserve(table->col_num_);
        for (uint32_t i = 0; i < insert_col_size; i++)
        {
            const query::Element& e = info.elements[i];
            ColumnInfo* col = table->col_name2col_[e.name];

            if (col->isPK()) {
                pk_col = i;
                pk_v = ast::vmVisitAtom(e.valueExpr);
            }

            elements.push_back(insert_element{
                page::range_t{ col->vEntry_offset_ ,col->str_len_ },
                ast::vmVisitAtom(e.valueExpr), col->col_t_, col->isFK(), col->other_value_
                });
        }

        // prepare default value
        for (auto const&[name, col] : table->col_name2col_)
        {
            if (col->isDEFAULT())
            {

                bool user_input_default = false;
                for (const query::Element& e : info.elements)
                    if (e.name == name) {
                        user_input_default = true;
                        break;
                    }
                if (user_input_default)
                    continue;

                ValueEntry dv = table->get_default_value(name);
                table::value_t v;
                if (col->col_t_ == col_t_t::INTEGER) {
                    v = page::read_int(dv.content_);
                }
                else {
                    v = std::string(dv.content_, col->str_len_);
                }
                elements.push_back(insert_element{
                    page::range_t{ col->vEntry_offset_ , col->str_len_ },
                    v, col->col_t_, false, page::NOT_A_PAGE
                    });
            }

        }

        // check FK
        for (const insert_element& e : elements) {
            if (e.fk) {
                if (e.col_t == col_t_t::INTEGER) {
                    int32_t i = std::get<int32_t>(e.value);
                    if (!table_pk_ref_INT[e.fk_table].count(i)) {
                        result.msg = "FK constraint violate: \"" + std::to_string(i) + "\"";
                        return;
                    }
                }
                else {
                    std::string s = std::get<std::string>(e.value);
                    if (!table_pk_ref_VARCHAR[e.fk_table].count(s)) {
                        result.msg = "FK constraint violate: \"" + s + "\"";
                        return;
                    }
                }
            }
        }

        // check PK, AUTOPK
        if (elements.size() != table->col_num_)
        {
            if (elements.size() + 1 != table->col_num_)
                debug::ERROR_LOG("INSERT VALUES prepare failure\n");

            // prepare AUTOPK
            if (pk_col != TableMetaPage::NOT_A_COLUMN)
                debug::ERROR_LOG("PK column error\n");

            // HACK: promise to be the last element for auto pk
            elements.push_back(insert_element{
                table->get_col_range(page::autoPK),
                table->get_auto_id(), col_t_t::INTEGER, false, NOT_A_PAGE
                });
        }
        else {
            // check PK
            const table::value_t& v = elements[pk_col].value;
            const insert_element& element = elements[pk_col];
            if (element.col_t == col_t_t::INTEGER) {
                int32_t i = std::get<int32_t>(v);
                if (table_pk_ref_INT[table->get_page_id()].count(i)) {
                    result.msg = "PK exists: \"" + std::to_string(i) + "\"";
                    return;
                }
            }
            else {
                std::string s = std::get<std::string >(v);
                if (table_pk_ref_VARCHAR[table->get_page_id()].count(s)) {
                    result.msg = "PK exists: \"" + s + "\"";
                    return;
                }
            }
        }


        // insert
        tree::KVEntry kv{};
        // prepare KeyEntry
        if (table->hasPK()) {
            if (table->PK_t() == key_t_t::INTEGER) {
                kv.kEntry.key_t = table->PK_t();
                kv.kEntry.key_int = std::get<int32_t>(pk_v);
            }
            else {
                kv.kEntry.key_t = table->PK_t();
                kv.kEntry.key_str = std::get<std::string>(pk_v);
            }
        }
        else {
            kv.kEntry.key_t = key_t_t::INTEGER;
            kv.kEntry.key_int = std::get<int32_t>(elements.back().value);
        }

        // prepare ValueEntry
        for (const insert_element& e : elements) {
            if (e.col_t == col_t_t::INTEGER) {
                update_vEntry(kv.vEntry, e.range, std::get<int32_t>(e.value));
            }
            else {
                update_vEntry(kv.vEntry, e.range, std::get<std::string>(e.value));
            }
        }

        // checked in PK view before
        if (!table->bt_->insert(kv))
            debug::ERROR_LOG("INSERT ERROR\n");

        // update PK view
        if (table->PK_t() == key_t_t::INTEGER) {
            int32_t i = std::get<int32_t>(pk_v);
            table_pk_ref_INT[table->get_page_id()][i] = NON_FK_REF; // set to 1
        }
        else {
            std::string s = std::get<std::string>(pk_v);
            table_pk_ref_VARCHAR[table->get_page_id()][s] = NON_FK_REF; // set to 1
        }
        // update FK view
        for (const insert_element& e : elements) {
            if (e.fk) {
                if (e.col_t == col_t_t::INTEGER) {
                    int32_t i = std::get<int32_t>(e.value);
                    table_pk_ref_INT[e.fk_table][i]++;
                }
                else {
                    std::string s = std::get<std::string>(e.value);
                    table_pk_ref_VARCHAR[e.fk_table][s]++;
                }
            }
        }

        result.msg = "INSERT OK";
    }

    void VM::doDelete(process_result_t& result, const query::DeleteInfo& info)
    {
        page::TableMetaPage* table = table_meta_[info.sourceTable];
        std::vector<std::string> colNames = table->cols_;
        std::vector<page::ColumnInfo> columnInfos;
        columnInfos.reserve(colNames.size());
        for (auto const& name : colNames) {
            columnInfos.push_back(*table->col_name2col_[name]);
        }
        table::TableInfo tableInfo(info.sourceTable, std::move(colNames), std::move(columnInfos), this);

        tree::BTree* bt = table->bt_;
        tree::BTit it = bt->range_query_from_begin();
        tree::BTit end = bt->range_query_from_end();
        std::deque<tree::KVEntry> to_be_deleted;

        std::unordered_map<int32_t, uint32_t>& pk_ref_INT =
            table_pk_ref_INT[table->get_page_id()];
        std::unordered_map<std::string, uint32_t>& pk_ref_VARCHAR =
            table_pk_ref_VARCHAR[table->get_page_id()];

        while (it != end)
        {
            ValueEntry vEntry = it.getV();
            row_view rv(tableInfo, vEntry);
            if (ast::vmVisit(info.whereExpr, rv)) {
                // check FK constraint
                bool ok_to_delete = true;
                KeyEntry kEntry = it.getK();
                if (kEntry.key_t == key_t_t::INTEGER) {
                    // some FK ref
                    if (pk_ref_INT[kEntry.key_int] != NON_FK_REF)
                        ok_to_delete = false;
                }
                else {
                    // some FK ref
                    if (pk_ref_VARCHAR[kEntry.key_str] != NON_FK_REF)
                        ok_to_delete = false;
                }
                if (ok_to_delete)
                    to_be_deleted.push_back({ kEntry, vEntry });
            }
            ++it;
        }

        // update PK ref
        struct fk_info_t {
            col_t_t col_t;
            page_id_t fk_table;
            range_t range;
        };
        std::vector<fk_info_t> fk_infos;
        for (auto const&[name, col] : table->col_name2col_) {
            if (col->isFK())
                fk_infos.push_back(fk_info_t{
                col->col_t_, col->other_value_,
                range_t{ col->vEntry_offset_, col->str_len_ } });
        }

        for (const tree::KVEntry& kv : to_be_deleted)
        {
            // erase from other table's pk view
            for (auto const& fk_info : fk_infos)
            {
                if (fk_info.col_t == col_t_t::INTEGER) {
                    int32_t i = page::get_range_INT(kv.vEntry, fk_info.range);
                    table_pk_ref_INT[fk_info.fk_table][i]--;
                }
                else {
                    std::string s = page::get_range_VARCHAR(kv.vEntry, fk_info.range);
                    table_pk_ref_VARCHAR[fk_info.fk_table][s]--;
                }
            }

            // erase from B+Tree
            bt->erase(kv.kEntry);

            // erase from pk view
            if (kv.kEntry.key_t == key_t_t::INTEGER)
                pk_ref_INT.erase(kv.kEntry.key_int);
            else
                pk_ref_VARCHAR.erase(kv.kEntry.key_str);
        }

        result.msg = "delete " + std::to_string(to_be_deleted.size()) + " rows";
    }



    //
    // 4 op node service providing for sql logic
    //

    VirtualTable VM::scanTable(const std::string& tableName) {
        page::TableMetaPage* table_page = table_meta_[tableName];
        std::vector<std::string> colNames = table_page->cols_;
        std::vector<page::ColumnInfo> columnInfos;
        columnInfos.reserve(colNames.size());
        for (auto const& name : colNames) {
            columnInfos.push_back(*table_page->col_name2col_[name]);
        }
        table::TableInfo tableInfo(tableName, std::move(colNames), std::move(columnInfos), this);

        table_view tv(tableInfo);
        VirtualTable vt(tv);
        std::future<void> no_use =
            register_task(std::mem_fn(&VM::doScanTable), this, vt, tableName);
        return vt;
    }

    void VM::doScanTable(VirtualTable ret, const std::string tableName) {
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



    VirtualTable VM::join(VirtualTable t1, VirtualTable t2, bool pk) {
        std::string _tableName;
        std::vector<std::string> _colNames;
        std::vector<page::ColumnInfo> _colInfos;
        uint32_t vEntry_offset = 0;
        auto table1 = t1.table_view_.table_info_;
        auto table2 = t2.table_view_.table_info_;
        _tableName = table1->tableName_ + " JOIN " + table2->tableName_;
        const uint32_t size1 = table1->colNames_.size();
        const uint32_t size2 = table2->colNames_.size();

        auto new_col_name = [](const std::string& tableName, const std::string& colName)->std::string
        {
            return tableName + "." + colName;
        };

        if (pk) {
            _tableName += " ON PK";
            _colNames.reserve(size1 + size2 - 1);
            _colInfos.reserve(size1 + size2 - 1);
            // a(id, name) b(id, name) // ignore the second PK name
            // (a.id, a.name, b.name)  // range_t -> range_t
            for (uint32_t i = 0; i < size1; i++) {
                _colNames.push_back(new_col_name(table1->tableName_, table1->colNames_[i]));
                _colInfos.push_back(table1->columnInfos_[i]);
                vEntry_offset = table1->columnInfos_[i].vEntry_offset_ + table1->columnInfos_[i].str_len_;
            }
            for (uint32_t i = 0; i < size2; i++) {
                if (i == table2->pk_col_)
                    continue;
                _colNames.push_back(new_col_name(table2->tableName_, table2->colNames_[i]));
                _colInfos.push_back(table2->columnInfos_[i]);
                _colInfos.back().vEntry_offset_ += vEntry_offset;
            }
        }
        else {
            _colNames.reserve(size1 + size2);
            _colInfos.reserve(size1 + size2);
            // a(id, name) b(id, name)
            // (a.id, a.name, b.id, b.name) // range_t -> <bool, range_t>
            //  true,  true,  false, false  // bool denotes whether it's the first table
            for (uint32_t i = 0; i < size1; i++) {
                _colNames.push_back(new_col_name(table1->tableName_, table1->colNames_[i]));
                _colInfos.push_back(table1->columnInfos_[i]);
                vEntry_offset = table1->columnInfos_[i].vEntry_offset_ + table1->columnInfos_[i].str_len_;
            }
            for (uint32_t i = 0; i < size2; i++) {
                _colNames.push_back(new_col_name(table2->tableName_, table2->colNames_[i]));
                _colInfos.push_back(table2->columnInfos_[i]);
                _colInfos.back().vEntry_offset_ += vEntry_offset;
            }
            if (table1->hasPK())
                _colInfos[table1->pk_col_].setNONPK();
            if (table2->hasPK())
                _colInfos[size1 + table2->pk_col_].setNONPK();
        }

        if (_colInfos.back().vEntry_offset_ + _colInfos.back().str_len_ > page::MAX_TUPLE_SIZE)
            debug::ERROR_LOG("exceed tuple size when joining \"%s\" and \"%s\" \n",
                table1->tableName_.c_str(), table2->tableName_.c_str());

        table::TableInfo tableInfo(std::move(_tableName), std::move(_colNames), std::move(_colInfos), this);
        VirtualTable vt(tableInfo);
        std::future<void> no_use =
            register_task(std::mem_fn(&VM::doJoin), this, vt, t1, t2, pk, size1, vEntry_offset);
        return vt;
    }

    void VM::doJoin(VirtualTable ret, VirtualTable t1, VirtualTable t2, bool pk, uint32_t table2_col_start, uint32_t vEntry_offset) {
        // splice 2 row into 1 row
        auto tableInfo = ret.table_view_.table_info_;
        const uint32_t col_size = tableInfo->colNames_.size();
        auto splice = [&tableInfo, col_size, table2_col_start, vEntry_offset](row_view r1, row_view r2) -> row_view
        {
            ValueEntry vEntry;
            vEntry.value_state_ = value_state::INUSED;
            for (uint32_t i = 0; i < col_size; i++) {
                const page::ColumnInfo& col = tableInfo->columnInfos_[i];
                range_t range{ col.vEntry_offset_, col.str_len_ };
                if (i < table2_col_start)
                    page::update_vEntry(vEntry, range, *r1.row_, range);
                else
                    page::update_vEntry(vEntry, range, *r2.row_, range_t{ range.begin - vEntry_offset, range.len });
            }
            return row_view(*tableInfo, vEntry);
        };

        if (pk) {
            // get col_t_ and pk ranges for pk_cmp
            auto table1_info = t1.table_view_.table_info_;
            auto table2_info = t2.table_view_.table_info_;

            const page::ColumnInfo& pk1_col = table1_info->columnInfos_[table1_info->pk_col_];
            const page::ColumnInfo& pk2_col = table2_info->columnInfos_[table2_info->pk_col_];

            col_t_t col_type = pk1_col.col_t_;

            range_t pk1_range{ pk1_col.vEntry_offset_, pk1_col.str_len_ };
            range_t pk2_range{ pk2_col.vEntry_offset_, pk2_col.str_len_ };
            // < 0
            // = 0
            // > 0
            auto pk_cmp = [col_type, pk1_range, pk2_range](row_view r1, row_view r2)->int32_t
            {
                if (col_type == col_t_t::INTEGER) {
                    int32_t pk1_value = get_range_INT(*r1.row_, pk1_range);
                    int32_t pk2_value = get_range_INT(*r2.row_, pk2_range);
                    return pk1_value - pk2_value;
                }
                else {
                    std::string pk1_value = get_range_VARCHAR(*r1.row_, pk1_range);
                    std::string pk2_value = get_range_VARCHAR(*r2.row_, pk2_range);
                    return pk1_value.compare(pk2_value);
                }
            };

            row_view r1 = t1.getRow();
            row_view r2 = t2.getRow();

            while (!r1.isEOF() && !r2.isEOF()) {
                const int32_t flag = pk_cmp(r1, r2);

                if (flag == 0) {
                    ret.addRow(splice(r1, r2));
                    r1 = t1.getRow();
                    r2 = t2.getRow();
                }
                else if (flag < 0) {
                    r1 = t1.getRow();
                }
                else {
                    r2 = t2.getRow();
                }
            }

            ret.addEOF();
        }
        else {
            std::deque<row_view> table1 = t1.waitAll();
            row_view r2 = t2.getRow();
            while (!r2.isEOF()) {
                for (row_view r1 : table1) {
                    if (!r1.isEOF())
                        ret.addRow(splice(r1, r2));
                }
                r2 = t2.getRow();
            }
            ret.addEOF();
        }
    }



    VirtualTable VM::projection(VirtualTable t, const std::vector<std::string>& colNames) {
        std::vector<std::string> _colNames;
        std::vector<page::ColumnInfo> _colInfos;
        std::vector<page::range_t> _origin_ranges;
        auto table_info_ = t.table_view_.table_info_;
        const uint32_t col_size = table_info_->colNames_.size();

        // TODO: maybe some compaction for projetion
        // TODO: (for subquery) promise the col offset is in asending order

        for (auto const& colName : colNames) {
            for (uint32_t i = 0; i < col_size; i++)
                if (colName == table_info_->colNames_[i]) {
                    _colNames.push_back(colName);
                    _colInfos.push_back(table_info_->columnInfos_[i]);
                    _origin_ranges.push_back(
                        { _colInfos.back().vEntry_offset_, _colInfos.back().str_len_ });
                }
        }

        table::TableInfo tableInfo(table_info_->tableName_,
            std::move(_colNames), std::move(_colInfos), this);
        VirtualTable vt(tableInfo);
        std::future<void> no_use =
            register_task(std::mem_fn(&VM::doProjection), this, vt, t, std::move(_origin_ranges));
        return vt;
    }

    void VM::doProjection(VirtualTable ret, VirtualTable t, const std::vector<page::range_t> origin_ranges) {
        auto table_info_ = ret.table_view_.table_info_;
        row_view rv = t.getRow();
        const uint32_t col_size = table_info_->columnInfos_.size();
        while (!rv.isEOF()) {
            ValueEntry vEntry = *rv.row_;
            for (uint32_t i = 0; i < col_size; i++) {
                page::range_t origin_range = origin_ranges[i];
                std::memcpy(vEntry.content_ + table_info_->columnInfos_[i].vEntry_offset_,
                    rv.row_->content_ + origin_range.begin,
                    origin_range.len);
            }
            update_vEntry(*rv.row_, vEntry);
            ret.addRow(rv);
            rv = t.getRow();
        }
        ret.addEOF();
    }



    VirtualTable VM::sigma(VirtualTable t, std::shared_ptr<ast::BaseExpr> whereExpr) {
        VirtualTable vt(t.table_view_);
        std::future<void> no_use =
            register_task(std::mem_fn(&VM::doSigma), this, vt, t, whereExpr);
        return vt;
    }

    void VM::doSigma(VirtualTable ret, VirtualTable t, std::shared_ptr<ast::BaseExpr> whereExpr) {
        row_view rv = t.getRow();
        while (!rv.isEOF()) {
            if (ast::vmVisit(whereExpr, rv)) {
                ret.addRow(rv);
            }
            rv = t.getRow();
        }
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
        for (auto&[name, table] : table_meta_)
            delete table;
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
        pkCol->vEntry_offset_ = 0;
        table->insert_column("tid", pkCol);

        ColumnInfo* col = new ColumnInfo;
        col->col_t_ = col_t_t::VARCHAR;
        col->str_len_ = 20;
        col->vEntry_offset_ = sizeof(int32_t);
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


    void VM::showDB() {
        printf("xjbDB has %d tables", db_meta_->table_num_);
        query_print_n();
        println();
        for (auto const&[name, table] : table_meta_)
        {
            query_print("table \"%s\"", name.c_str());
            query_print_n();

            // prepare colInfo
            struct output_t {
                col_t_t col_t;
                range_t range;
            };
            std::vector<output_t> outputCol;
            outputCol.reserve(table->col_num_);
            query_print("      ");
            for (auto const& colName : table->cols_) {
                const ColumnInfo* col = table->col_name2col_[colName];
                query_print("%s\t", colName.c_str());
                outputCol.push_back(output_t{
                    col->col_t_, range_t{ col->vEntry_offset_, col->str_len_ }
                    });
            }
            query_print_n();
            println();

            uint32_t cnt = 0;
            table->bt_->range_query_begin_lock();
            auto it = table->bt_->range_query_from_begin();
            auto end = table->bt_->range_query_from_end();
            while (it != end)
            {
                if (table->bt_->key_t() == page::key_t_t::INTEGER)
                    query_print("%d -> ", it.getK().key_int);
                else
                    query_print("%s -> ", it.getK().key_str.c_str());

                ValueEntry vEntry = it.getV();
                for (output_t output : outputCol) {
                    if (output.col_t == col_t_t::INTEGER) {
                        query_print("%d\t", get_range_INT(vEntry, output.range));
                    }
                    else {
                        std::string s = get_range_VARCHAR(vEntry, output.range);
                        query_print("%s\t", s.c_str());
                    }
                }
                query_print_n();

                ++it;
                cnt++;
            }
            table->bt_->range_query_end_unlock();
            query_print("output size = %d", cnt);
            query_print_n();
            println();

            // show PK view
            query_print("PK view:");
            query_print_n();
            if (table->bt_->key_t() == key_t_t::INTEGER) {
                const std::unordered_map<int32_t, uint32_t>& pk_ref
                    = table_pk_ref_INT[table->get_page_id()];
                for (auto const&[key, ref] : pk_ref) {
                    query_print("%d\t->\t%d", key, ref);
                    query_print_n();
                }
            }
            else {
                const std::unordered_map<std::string, uint32_t>& pk_ref
                    = table_pk_ref_VARCHAR[table->get_page_id()];
                for (auto const&[key, ref] : pk_ref) {
                    query_print("%s\t->\t%d", key.c_str(), ref);
                    query_print_n();
                }
            }
            println();

        } // end iterate all tables
    } // end showDB();

    void VM::query_print_n() {
        printf("\n");
        output_line_start = true;
    }

    void VM::println() {
        query_print("------------------------------------------------------\n");
        output_line_start = true;
    }

} // end namespace DB::vm