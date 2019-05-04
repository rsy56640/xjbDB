#include "include/table.h"
#include "include/BplusTree.h"
#include "include/debug_log.h"
#include "include/vm.h"

namespace DB::table
{

    TableInfo::TableInfo() {}

    TableInfo::TableInfo(const std::string& tableName,
        std::vector<std::string> colNames,
        std::vector<page::ColumnInfo> columnInfos,
        vm::VM* vm)
        :
        tableName_(tableName),
        colNames_(std::move(colNames)),
        columnInfos_(std::move(columnInfos))
    {
        reset(vm);
    }


    void TableInfo::reset(vm::VM* vm)
    {
        table_pk_ref_INT = &(vm->table_pk_ref_INT);
        table_pk_ref_VARCHAR = &(vm->table_pk_ref_VARCHAR);

        fks_.clear();
        for (uint32_t i = 0, size = columnInfos_.size(); i < size; ++i)
        {
            page::ColumnInfo& column = columnInfos_[i];
            if (column.isPK()) {
                if (pk_col_ == page::TableMetaPage::NOT_A_COLUMN)
                    pk_col_ = i;
                else
                    debug::ERROR_LOG("no support for multiple PKs\n");
            }
            if (column.isFK())
                fks_.push_back({ i, column.other_value_ });
        }
    }

    bool TableInfo::hasPK() const { return pk_col_ != page::TableMetaPage::NOT_A_COLUMN; }

    page::key_t_t TableInfo::PK_t() const {
        if (hasPK()) {
            const page::ColumnInfo& col = columnInfos_[pk_col_];
            return col.col_t_;
        }
        else
            return page::key_t_t::INTEGER;
    }

    uint32_t TableInfo::str_len() const {
        if (hasPK())
            return  columnInfos_[pk_col_].str_len_;
        else
            return 0;
    }


    // return NOT_A_PAGE if not fk
    page::page_id_t TableInfo::fk_ref_table_id(uint32_t fk_col) {
        for (auto[fkCol, table_id] : fks_) {
            if (fkCol == fk_col)
                return table_id;
        }
        return page::NOT_A_PAGE;
    }



    table_view::table_view() {}
    table_view::table_view(const TableInfo& tableInfo)
    {
        table_info_ = std::make_shared<const TableInfo>(tableInfo);
    }




    row_view::row_view(table_view table_view, row_t row)
        :eof_(false), table_view_(table_view), row_(std::make_shared<row_t>(row)) {}

    bool row_view::isEOF() const { return eof_; }

    void row_view::setEOF() { eof_ = true; }

    //value_t row_view::getValue(col_name_t colName) const {
    //}



    VirtualTable::VirtualTable(table_view table_view)
        :table_view_(table_view), ch_(std::make_shared<channel_t>()) {}

    void VirtualTable::addRow(row_view row) {
        std::lock_guard<std::mutex> lg{ ch_->mtx_ };
        ch_->row_buffer_.push(row);
        ch_->cv_.notify_one();
    }

    void VirtualTable::addEOF() {
        row_view eof_row(table_view_, {});
        eof_row.setEOF();
        std::lock_guard<std::mutex> lg{ ch_->mtx_ };
        ch_->row_buffer_.push(eof_row);
        ch_->cv_.notify_one();
    }

    void VirtualTable::set_size(uint32_t size) { size_ = size; }

    uint32_t VirtualTable::size() const { return size_; }

    row_view VirtualTable::getRow() {
        std::unique_lock<std::mutex> ulk{ ch_->mtx_ };
        ch_->cv_.wait(ulk, [this]() { return !ch_->row_buffer_.empty(); });
        row_view row = ch_->row_buffer_.front();
        ch_->row_buffer_.pop();
        return row;
    }

    std::deque<row_view> VirtualTable::waitAll() {
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





} // end namespace DB::table