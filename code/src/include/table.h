#ifndef _TABLE_H
#define _TABLE_H
#include <string>
#include <variant>
#include <vector>
#include <unordered_map>
#include <utility>
#include <memory>
#include "env.h"
#include "page.h"
#include "buffer_pool.h"

namespace DB::vm { class VM; }

namespace DB::table
{

    using value_t = std::variant<int32_t, std::string>;


    struct TableInfo
    {
        TableInfo(std::string tableName,
            std::vector<page::ColumnInfo> columnInfos,
            std::vector<std::string> colNames,
            vm::VM* vm);

        void reset(vm::VM* vm);

        // return NOT_A_PAGE if not fk
        page::page_id_t fk_ref_table_id(uint32_t fk_col);

        // init from outside
        std::string tableName_;
        std::vector<page::ColumnInfo> columnInfos_;
        std::vector<std::string> colNames_;
        const std::unordered_map<page::page_id_t,
            std::unordered_set<int32_t>>*table_key_index_INT;              // PK view
        const std::unordered_map<page::page_id_t,
            std::unordered_set<std::string>>*table_key_index_VARCHAR;      // PK view

        // should init
        uint32_t pk_col_ = page::TableMetaPage::NOT_A_COLUMN;
        std::vector<std::pair<uint32_t, page::page_id_t>> fks_; // fk col -> table meta page
    };


    struct table_view
    {
        table_view() {}


        std::shared_ptr<const TableInfo> table_info_;
    };


} // end namespace DB::table

#endif // !_TABLE_H
