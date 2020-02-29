//
// Created by 刘瑞康 on 2020/2/18.
//

#pragma once

#include "sql_expr.h"
#include "page.h"
#include "table.h"
#include <memory>
#include <vector>
#include <unordered_map>
#include <unordered_set>

using std::vector;
using std::shared_ptr;
using std::unordered_map;
using std::pair;

/*
 * this file includes
 *  node structs of ap ast
 *  functions for ap ast
 */

namespace std {
    template<>
    struct hash<pair<string, string>> {
        using result_type = std::size_t;
        using argument_type = pair<string, string>;
        result_type operator()(const argument_type& attr) const {
            result_type h1 = std::hash<std::string>{}(attr.first);
            result_type h2 = std::hash<std::string>{}(attr.second);
            return h1 ^ (h2 << 1);
        }
    };
    template<>
    struct hash<DB::page::range_t> {
        using result_type = std::size_t;
        using argument_type = DB::page::range_t;
        result_type operator()(DB::page::range_t range) const {
            result_type h1 = std::hash<uint32_t>{}(range.begin);
            result_type h2 = std::hash<uint32_t>{}(range.len);
            return h1 ^ (h2 << 1);
        }
    };
}

namespace DB::ast {

    //op nodes of ap
    enum class ap_op_t_t { EMIT, PROJECT, FILTER, JOIN, TABLE };

    using col_name_t = pair<string, string>;

    class APMap {
    public:
        // init from source table
        APMap() {}
        APMap(const table::TableInfo& table);
        void join(const APMap& right);
        page::range_t get(const col_name_t&);
        uint32_t len() const;
        bool check_unique(page::range_t) const;
    private:
        unordered_map<col_name_t, page::range_t> attr_map{};
        uint32_t tuple_len;
        std::unordered_set<page::range_t> unique_ranges_{};
    };

    struct APBaseOp {
        APBaseOp(ap_op_t_t op_t, APBaseOp *parentOp = nullptr)
            : op_t_(op_t), _parentOp(parentOp) {}
        virtual ~APBaseOp() = 0;

        virtual bool isJoin() { return false; };

        // used to traversal
        virtual void produce() {};
        // source are used to check left/right child, map is constructed from bottom to top
        virtual void consume(APBaseOp *source, APMap &map) {};

        void setParentOp(APBaseOp *parentOp) { _parentOp = parentOp; };

        ap_op_t_t op_t_;
        APBaseOp* _parentOp;
    };

    struct APEmitOp : public  APBaseOp {
        APEmitOp(APBaseOp *table, int hashTableCount, int tableCount)
            : APBaseOp(ap_op_t_t::EMIT), _table(table), _hashTableCount(hashTableCount), _tableCount(tableCount) {}
        virtual ~APEmitOp() {}

        virtual void produce();
        virtual void consume(APBaseOp *source, APMap &map);

        int _hashTableCount;
        int _tableCount;
        APBaseOp *_table;
    };

    struct APProjectOp : public APBaseOp {
        APProjectOp() : APBaseOp(ap_op_t_t::PROJECT) {}
        virtual ~APProjectOp() { }

        virtual void produce(){/*TODO*/};
        virtual void consume(APBaseOp *source, APMap &map){/*TODO*/};
    };

    struct APFilterOp : public APBaseOp {
        APFilterOp(APBaseOp *table, shared_ptr<BaseExpr> condition)
            : APBaseOp(ap_op_t_t::FILTER), _table(table), _condition(condition) {}
        virtual ~APFilterOp() { }

        virtual void produce();
        virtual void consume(APBaseOp *source, APMap &map);

        APBaseOp *_table;
        shared_ptr<BaseExpr> _condition;
    };

    struct APJoinOp : public APBaseOp {
        APJoinOp(APBaseOp *tableLeft, APBaseOp *tableRight, col_name_t leftAttr, col_name_t rightAttr, int hashTableIndex)
            : APBaseOp(ap_op_t_t::JOIN), _tableLeft(tableLeft), _tableRight(tableRight),
              _leftAttr(leftAttr), _rightAttr(rightAttr), _hashTableIndex(hashTableIndex) {}
        virtual ~APJoinOp() {}

        virtual bool isJoin() { return true; };

        virtual void produce();
        virtual void consume(APBaseOp *source, APMap &map);

        int _hashTableIndex;
        APBaseOp *_tableLeft;
        APBaseOp *_tableRight;
        col_name_t _leftAttr;
        col_name_t _rightAttr;
        APMap _leftMap;
    };

    struct APTableOp : public APBaseOp {
        APTableOp(const table::TableInfo& table, string tableName, int tableIndex);
        virtual ~APTableOp() {}

        virtual void produce();

        string _tableName;
        int _tableIndex;
        APMap _map;
    };


    /*
     * CheckVisit
     * functions
     *  root type must be comparison
     *  finish constant computation in place
     *  check existence of column
     * used before actually generation
     * throw exception if check fails
     */
    void apCheckSingle(shared_ptr<BaseExpr> &condition);

    void apCheckVisit(vector<shared_ptr<BaseExpr>> &conditions);


    using Column = std::pair<string, string>;

    /*
     * functions
     *  generate ast for compile
     *  default select all
     *  currently don't handle select
     */
    shared_ptr<APEmitOp> generateAst(const vector<string> &tables, const vector<shared_ptr<BaseExpr>> &conditions, vector<Column> columns = {});

    /*
     * functions
     *  generate code from ast
     */
    string generateCode(shared_ptr<APEmitOp> emit);
}
