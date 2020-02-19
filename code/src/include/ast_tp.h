#pragma once

#include "table.h"
#include "sql_expr.h"
#include <variant>

/*
 * this file includes
 *  node structs of tp ast
 *  functions for tp ast
 */

namespace DB::ast {

    enum class tp_op_t_t { PROJECT, FILTER, JOIN, TABLE };
    struct TPBaseOp {
        TPBaseOp(tp_op_t_t op_t) : op_t_(op_t) {}
        virtual ~TPBaseOp() = 0;
        virtual table::VirtualTable getOutput() = 0;

        tp_op_t_t op_t_;
    };

    struct TPProjectOp : public TPBaseOp {
        TPProjectOp() : TPBaseOp(tp_op_t_t::PROJECT) {}
        virtual ~TPProjectOp() { }
        virtual table::VirtualTable getOutput();

        std::vector<std::string> _names;	//the name of the accordingly element, not sure if useful
        std::vector<std::shared_ptr<ast::AtomExpr>> _elements;	//if empty, $ are used, all columns are needed
        std::shared_ptr<TPBaseOp> _source;
    };

    struct TPFilterOp : public TPBaseOp {
        TPFilterOp(ast::BaseExpr* whereExpr) : TPBaseOp(tp_op_t_t::FILTER), _whereExpr(whereExpr) {}
        virtual ~TPFilterOp() { }
        virtual table::VirtualTable getOutput();

        std::shared_ptr<ast::BaseExpr> _whereExpr;
		std::shared_ptr<TPBaseOp> _source;
    };

    struct TPJoinOp : public TPBaseOp {
        TPJoinOp() : TPBaseOp(tp_op_t_t::JOIN) {}
        virtual ~TPJoinOp() {}
        virtual table::VirtualTable getOutput();

        std::vector<std::shared_ptr<TPBaseOp>> _sources;	//currently suppose all sources are TPTableOp
        bool isJoin;	//even it's true, not sure if the tables can be joined
    };

    struct TPTableOp : public TPBaseOp {
        TPTableOp(const std::string tableName) : TPBaseOp(tp_op_t_t::TABLE), _tableName(tableName) {}
        virtual ~TPTableOp() {}
        virtual table::VirtualTable getOutput();

        std::string _tableName;
    };

    //===========================================================
    //visit functions


    void tpOutputVisit(std::shared_ptr<const TPBaseOp> root, std::ostream &os);

    /*
    *check visit, used in parsing phase
    *	1. check sql semantics,
    *		such as TableA.idd == 123 where there doesn't exist idd column in TableA
    *	2. check the type matching,
    *		such as WHERE "wtf" AND TableA.name == 123,
    *		here string "wtf" is not supported by logicalOp AND,
    *		string TableA.name and number 123 don't matched either
    *
    *for any dismatching, throw DB_Exception
    */

    //check WHERE clause(expression)
    void tpCheckVisit(std::shared_ptr<const BaseExpr> root, string tableName = std::string());

    //check others(expressionAtom)
    void tpCheckVisit(std::shared_ptr<const AtomExpr> root, const std::string tableName = std::string());


    /*
    *vm visit, for vm
    *guarantee no except
    */

    //for WHERE clause(expression), used for filtering values of a row
    bool vmVisit(std::shared_ptr<const BaseExpr> root, table::row_view row);

    //for others(expressionAtom), used for computing math/string expression and data in the specified row
    table::value_t vmVisitAtom(std::shared_ptr<const AtomExpr> root, table::row_view row = NULL_ROW);
}








