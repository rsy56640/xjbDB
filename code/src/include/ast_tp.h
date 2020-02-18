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

    enum class op_t_t { PROJECT, FILTER, JOIN, TABLE };
    struct BaseOp {
        BaseOp(op_t_t op_t) :op_t_(op_t) {}
        virtual ~BaseOp() = 0;
        virtual table::VirtualTable getOutput() = 0;

        op_t_t op_t_;
    };

    struct ProjectOp : public BaseOp {
        ProjectOp() :BaseOp(op_t_t::PROJECT) {}
        virtual ~ProjectOp() { }
        virtual table::VirtualTable getOutput();

        std::vector<std::string> _names;	//the name of the accordingly element, not sure if useful
        std::vector<std::shared_ptr<ast::AtomExpr>> _elements;	//if empty, $ are used, all columns are needed
        std::shared_ptr<BaseOp> _source;
    };

    struct FilterOp : public BaseOp {
        FilterOp(ast::BaseExpr* whereExpr) :BaseOp(op_t_t::FILTER), _whereExpr(whereExpr) {}
        virtual ~FilterOp() { }
        virtual table::VirtualTable getOutput();

        std::shared_ptr<ast::BaseExpr> _whereExpr;
		std::shared_ptr<BaseOp> _source;
    };

    struct JoinOp : public BaseOp {
        JoinOp() :BaseOp(op_t_t::JOIN) {}
        virtual ~JoinOp() {}
        virtual table::VirtualTable getOutput();

        std::vector<std::shared_ptr<BaseOp>> _sources;	//currently suppose all sources are TableOp
        bool isJoin;	//even it's true, not sure if the tables can be joined
    };

    struct TableOp : public BaseOp {
        TableOp(const std::string tableName) : BaseOp(op_t_t::TABLE), _tableName(tableName) {}
        virtual ~TableOp() {}
        virtual table::VirtualTable getOutput();

        std::string _tableName;
    };

    //===========================================================
    //visit functions

    /*
    *output visit, output the ast to the given ostream
    *regardless of validity
    */
    void tpOutputVisit(std::shared_ptr<const BaseExpr> root, std::ostream &os);

    void tpOutputVisit(std::shared_ptr<const BaseOp> root, std::ostream &os);

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
    *if passing, it is guaranteed other visit function will never encounter unexcepted cases
    *
    *the visited expr won't be modified at present,
    *may be modified for optimization in the future
    */

    //check WHERE clause(expression)
    void tpCheckVisit(std::shared_ptr<const BaseExpr> root, string tableName = std::string());

    //check others(expressionAtom)
    void tpCheckVisit(std::shared_ptr<const AtomExpr> root, const std::string tableName = std::string());


    /*
    *vm visit, for vm
    *guarantee no except
    */
    inline int numericOp(int op1, int op2, math_t_t math_t);
    inline bool comparisonOp(int op1, int op2, comparison_t_t comparison_t);

    //for WHERE clause(expression), used for filtering values of a row
    bool vmVisit(std::shared_ptr<const BaseExpr> root, table::row_view row);

    //for others(expressionAtom), used for computing math/string expression and data in the specified row
    table::value_t vmVisitAtom(std::shared_ptr<const AtomExpr> root, table::row_view row = NULL_ROW);
}








