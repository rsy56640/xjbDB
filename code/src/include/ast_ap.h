//
// Created by 刘瑞康 on 2020/2/18.
//

#pragma once

#include "sql_expr.h"
#include <memory>
#include <vector>
#include <map>

using std::vector;
using std::shared_ptr;
using std::map;
using std::pair;

/*
 * this file includes
 *  node structs of ap ast
 *  functions for ap ast
 */

namespace DB::ast{

    //op nodes of ap
    enum class ap_op_t_t { EMIT, PROJECT, FILTER, JOIN, TABLE };

    using APMap = map<pair<string, string>, int>; // need reset

    struct APBaseOp {
        APBaseOp(ap_op_t_t op_t, APBaseOp *parentOp = nullptr)
            : op_t_(op_t), _parentOp(parentOp) {}
        virtual ~APBaseOp() = 0;

        virtual void produce() {};
        // source are used to check left/right child, map is constructed from bottom to top
        virtual void consume(APBaseOp *source, APMap &map) {};

        void setParentOp(APBaseOp *parentOp) { _parentOp = parentOp; };

        ap_op_t_t op_t_;
        APBaseOp* _parentOp;
    };

    struct APEmitOp : public  APBaseOp {
        APEmitOp(APBaseOp *op, int hashTableCount)
            : APBaseOp(ap_op_t_t::EMIT), _op(op), _hashTableCount(hashTableCount) {}
        virtual ~APEmitOp() {}

        int _hashTableCount;
        APBaseOp *_op;
    };

    struct APProjectOp : public APBaseOp {
        APProjectOp() : APBaseOp(ap_op_t_t::PROJECT) {}
        virtual ~APProjectOp() { }

    };

    struct APFilterOp : public APBaseOp {
        APFilterOp(APBaseOp *tableOp, shared_ptr<BaseExpr> condition)
            : APBaseOp(ap_op_t_t::FILTER), _condition(condition) {}
        virtual ~APFilterOp() { }

        shared_ptr<BaseExpr> _condition;
    };

    struct APJoinOp : public APBaseOp {
        APJoinOp(APBaseOp *table1, APBaseOp *table2, shared_ptr<BaseExpr> condition, int hashTableIndex)
            : APBaseOp(ap_op_t_t::JOIN), _table1(table1), _table2(table2),
            _condition(condition), _hashTableIndex(hashTableIndex) {}
        virtual ~APJoinOp() {}

        int _hashTableIndex;
        APBaseOp *_table1;
        APBaseOp *_table2;
        shared_ptr<BaseExpr> _condition;
    };

    struct APTableOp : public APBaseOp {
        APTableOp(const string tableName);
        virtual ~APTableOp() {}
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


}
