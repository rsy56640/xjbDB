//
// Created by 刘瑞康 on 2020/2/18.
//

#pragma once

#include <string>
#include <memory>
#include <iostream>
#include <set>


/*
 * this file includes
 *  structs for expressions in sql
 */

namespace DB::ast{

    using std::string;
    using std::shared_ptr;
    using std::set;

    enum class base_t_t { LOGICAL_OP, COMPARISON_OP, ID, NUMERIC, STR, MATH_OP };
    enum class logical_t_t { AND, OR };
    enum class comparison_t_t { EQ, NEQ, LESS, GREATER, LEQ, GEQ, };
    enum class math_t_t { ADD, SUB, MUL, DIV, MOD, };

    // for print
    const string base2str[] = { "LOGICAL_OP", "COMPARISON_OP", "ID", "NUMERIC", "STR", "MATH_OP" };
    const string logical2str[] = { "AND", "OR" };
    const string comparison2str[] = { "==", "!=", "<", ">", "<=", ">=" };
    const string math2str[] = { "+", "-", "*", "/", "%" };

    struct BaseExpr {
        BaseExpr(base_t_t base_t) :base_t_(base_t) {}
        virtual ~BaseExpr() {};

        virtual set<string> getTables() { return {}; };
        virtual bool isJoin() { return false; };

        const base_t_t  base_t_;
    };

    struct NonAtomExpr : public BaseExpr {
        NonAtomExpr(base_t_t base_t) :BaseExpr(base_t) {}
        virtual ~NonAtomExpr() {};
    };

    struct AtomExpr : public BaseExpr {
        AtomExpr(base_t_t base_t) :BaseExpr(base_t) {}
        virtual ~AtomExpr() {};
    };

    struct LogicalOpExpr : public NonAtomExpr {
        LogicalOpExpr(logical_t_t logical_t, BaseExpr* left, BaseExpr* right) :
                NonAtomExpr(base_t_t::LOGICAL_OP), logical_t_(logical_t), _left(left), _right(right) {}
        virtual ~LogicalOpExpr(){};

        const logical_t_t logical_t_;
        shared_ptr<BaseExpr> _left;
        shared_ptr<BaseExpr> _right;
    };

    struct ComparisonOpExpr : public NonAtomExpr {
        //left,right must be AtomExpr*
        ComparisonOpExpr(comparison_t_t comparison_t, AtomExpr* left, AtomExpr* right) :
                NonAtomExpr(base_t_t::COMPARISON_OP), comparison_t_(comparison_t), _left(left), _right(right) {}
        virtual ~ComparisonOpExpr(){};

        virtual set<string> getTables()
        {
            auto t1 = _left->getTables();
            auto t2 = _right->getTables();
            t1.insert(t2.begin(), t2.end());
            return t1;
        }

        virtual bool isJoin() { return comparison_t_ == comparison_t_t::EQ; };

        const comparison_t_t comparison_t_;
        shared_ptr<BaseExpr> _left;
        shared_ptr<BaseExpr> _right;
    };

    struct MathOpExpr : public AtomExpr {
        MathOpExpr(math_t_t math_t, AtomExpr* left, AtomExpr* right) :
                AtomExpr(base_t_t::MATH_OP), math_t_(math_t), _left(left), _right(right) {}
        virtual ~MathOpExpr(){};

        virtual set<string> getTables()
        {
            auto t1 = _left->getTables();
            auto t2 = _right->getTables();
            t1.insert(t2.begin(), t2.end());
            return t1;
        }

        const math_t_t math_t_;
        shared_ptr<BaseExpr> _left;
        shared_ptr<BaseExpr> _right;
    };

    struct IdExpr : public AtomExpr {
        IdExpr(string columnName, string tableName = string()) :
                AtomExpr(base_t_t::ID), _tableName(tableName), _columnName(columnName) {}
        virtual ~IdExpr(){};

        virtual set<string> getTables(){ return {_tableName}; };

        const string getFullColumnName() const
        {
            if (_tableName.empty())
                return _columnName;
            else
                return _tableName + "." + _columnName;
        }

        string _tableName;
        string _columnName;
    };

    struct NumericExpr : public AtomExpr {
        NumericExpr(int value) :
                AtomExpr(base_t_t::NUMERIC), _value(value) {}
        virtual ~NumericExpr(){};

        int _value;
    };

    struct StrExpr : public AtomExpr {
        StrExpr(const string& value) :
                AtomExpr(base_t_t::STR), _value(value) {}
        virtual ~StrExpr(){};

        string _value;
    };


    int numericOp(int op1, int op2, math_t_t math_t);

    bool comparisonOp(int op1, int op2, comparison_t_t comparison_t);

    void _exprOutputVisit(shared_ptr<const BaseExpr> root, std::ostream& os, size_t indent);

    void exprOutputVisit(shared_ptr<const BaseExpr> root, std::ostream &os);

}
