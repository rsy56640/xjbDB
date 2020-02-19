//
// Created by 刘瑞康 on 2020/2/18.
//

#pragma once

#include "sql_expr.h"
#include <memory>
#include <vector>

using std::vector;
using std::shared_ptr;

/*
 * this file includes
 *  node structs of ap ast
 *  functions for ap ast
 */

namespace DB::ast{

    //op nodes of ap
    enum class ap_op_t_t { EMIT, PROJECT, FILTER, JOIN, TABLE };

    struct APBaseOp {
        APBaseOp(ap_op_t_t op_t) : op_t_(op_t) {}
        virtual ~APBaseOp() = 0;

        //virtual void produce() {};
        //virtual void consume(attributes,source) {};

        ap_op_t_t op_t_;
    };

    struct APEmitOp : public  APBaseOp {
        APEmitOp() : APBaseOp(ap_op_t_t::EMIT) {}
        virtual ~APEmitOp() {}

    };

    struct APProjectOp : public APBaseOp {
        APProjectOp() : APBaseOp(ap_op_t_t::PROJECT) {}
        virtual ~APProjectOp() { }

    };

    struct APFilterOp : public APBaseOp {
        APFilterOp(ast::BaseExpr* whereExpr) : APBaseOp(ap_op_t_t::FILTER) {}
        virtual ~APFilterOp() { }

    };

    struct APJoinOp : public APBaseOp {
        APJoinOp() : APBaseOp(ap_op_t_t::JOIN) {}
        virtual ~APJoinOp() {}

    };

    struct APTableOp : public APBaseOp {
        APTableOp(const string tableName) : APBaseOp(ap_op_t_t::TABLE), _tableName(tableName) {}
        virtual ~APTableOp() {}

        string _tableName;
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
    void apCheckVisit(vector<shared_ptr<const BaseExpr>> &conditions);


    using Column = std::pair<string, string>;

    /*
     * functions
     *  generate ast for compile
     *  default select all
     *  currently don't handle select
     */
    void generateAst(vector<string> tables, vector<shared_ptr<BaseExpr>> &conditions, vector<Column> columns = {});


}
