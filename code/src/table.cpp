#include "include/table.h"
#include "include/BplusTree.h"
#include "include/debug_log.h"
#include "include/vm.h"

namespace DB::table
{

    TableInfo::TableInfo(std::string tableName,
        std::vector<page::ColumnInfo> columnInfos,
        std::vector<std::string> colNames,
        vm::VM* vm)
        :
        tableName_(std::move(tableName)),
        columnInfos_(std::move(columnInfos)),
        colNames_(std::move(colNames)),
        table_pk_ref_INT(&(vm->table_pk_ref_INT)),
        table_pk_ref_VARCHAR(&(vm->table_pk_ref_VARCHAR))
    {
        reset(vm);
    }


    void TableInfo::reset(vm::VM* vm)
    {
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


    // return NOT_A_PAGE if not fk
    page::page_id_t TableInfo::fk_ref_table_id(uint32_t fk_col) {
        for (auto[fkCol, table_id] : fks_) {
            if (fkCol == fk_col)
                return table_id;
        }
        return page::NOT_A_PAGE;
    }


} // end namespace DB::table