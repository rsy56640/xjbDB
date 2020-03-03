#ifndef _TABLE_H
#define _TABLE_H
#include <string>
#include <variant>
#include <vector>
#include <unordered_map>
#include <queue>
#include <utility>
#include <memory>
#include "env.h"
#include "page.h"
#include "buffer_pool.h"

namespace DB::vm { class VM; }

namespace DB::table
{

    using value_t = std::variant<int32_t, std::string>;

    extern vm::VM* vm_; // define as nullptr in table.cpp

#define NULL_ROW table::row_view{ {}, {} }

    struct TableInfo
    {
        TableInfo();
        TableInfo(const std::string& tableName,
            std::vector<std::string> colNames,
            std::vector<page::ColumnInfo> columnInfos,
            vm::VM* vm);
        ~TableInfo() = default;
        TableInfo(const TableInfo&) = default;
        TableInfo& operator=(const TableInfo&) = default;
        TableInfo(TableInfo&&) = default;
        TableInfo& operator=(TableInfo&&) = default;

        void reset(vm::VM* vm);

        bool hasPK() const;
        page::key_t_t PK_t() const;
        uint32_t str_len() const;

        // return NOT_A_PAGE if not fk
        page::page_id_t fk_ref_table_id(uint32_t fk_col);

        // init from outside
        std::string tableName_;
        std::vector<std::string> colNames_;
        std::vector<page::ColumnInfo> columnInfos_; // col offset is in asending order !!!
        const std::unordered_map<page::page_id_t,
            std::unordered_map<int32_t, uint32_t>>*table_pk_ref_INT;
        const std::unordered_map<page::page_id_t,
            std::unordered_map<std::string, uint32_t>>*table_pk_ref_VARCHAR;

        // should init
        uint32_t pk_col_ = page::TableMetaPage::NOT_A_COLUMN;
        std::vector<std::pair<uint32_t, page::page_id_t>> fks_; // fk col -> table meta page
    };


    struct table_view
    {
        table_view();
        table_view(const TableInfo& tableInfo);

        std::shared_ptr<const TableInfo> table_info_;
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

        table_view table_view_;

    public:
        bool eof_;
        std::shared_ptr<row_t>  row_;
    };




    // used as a temporary table.
    // implementation might be stream ?? with some sync mechanism.
    class VirtualTable
    {
        struct channel_t {
            std::queue<row_view> row_buffer_;
            std::mutex mtx_;
            std::condition_variable cv_;
        };

    public:

        VirtualTable(table_view);
        VirtualTable(const VirtualTable&) = default;
        VirtualTable& operator=(const VirtualTable&) = default;

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



    static std::unordered_map<std::string, table::TableInfo> tableBuffer;

    table::TableInfo getTableInfo(const std::string& tableName);

    page::ColumnInfo getColumnInfo(const std::string& tableName, const std::string& columnName);


    struct attr_t {
        std::string attr_name_;
        page::range_t attr_ranges_;
    };
    struct schema_t {
        std::vector<attr_t> attrs_;
    };


} // end namespace DB::table

#endif // !_TABLE_H
