//
// Created by 刘瑞康 on 2020/2/19.
//

#include "sql_expr.h"

namespace DB::ast{

    int numericOp(int op1, int op2, math_t_t math_t)
    {
        switch (math_t)
        {
            case math_t_t::ADD:
                return op1 + op2;
            case math_t_t::SUB:
                return op1 - op2;
            case math_t_t::MUL:
                return op1 * op2;
            case math_t_t::DIV:
                return op1 / op2;
            case math_t_t::MOD:
                return op1 % op2;
            default:
                // unexpected
                throw std::string("wrong math operation type");
        }
    }

    bool comparisonOp(int op1, int op2, comparison_t_t comparison_t)
    {
        switch (comparison_t)
        {
            case comparison_t_t::EQ:
                return op1 == op2;
            case comparison_t_t::NEQ:
                return op1 != op2;
            case comparison_t_t::LESS:
                return op1 < op2;
            case comparison_t_t::GREATER:
                return op1 > op2;
            case comparison_t_t::LEQ:
                return op1 <= op2;
            case comparison_t_t::GEQ:
                return op1 >= op2;
            default:
                // unexpected
                throw std::string("wrong comparison operation type");
        }
    }

    void _exprOutputVisit(std::shared_ptr<const BaseExpr> root, std::ostream& os, size_t indent)
    {
        base_t_t base_t = root->base_t_;
        os << std::string(indent * 2, '-') << base2str[int(base_t)] << "  ";
        ++indent;
        switch (base_t)
        {
            case base_t_t::LOGICAL_OP:
            {
                std::shared_ptr<const LogicalOpExpr> logicalPtr = std::static_pointer_cast<const LogicalOpExpr>(root);
                os << logical2str[int(logicalPtr->logical_t_)] << std::endl;
                _exprOutputVisit(logicalPtr->_left, os, indent);
                _exprOutputVisit(logicalPtr->_right, os, indent);
            }
                break;
            case base_t_t::COMPARISON_OP:
            {
                std::shared_ptr<const ComparisonOpExpr> comparisonPtr = std::static_pointer_cast<const ComparisonOpExpr>(root);
                os << comparison2str[int(comparisonPtr->comparison_t_)] << std::endl;
                _exprOutputVisit(comparisonPtr->_left, os, indent);
                _exprOutputVisit(comparisonPtr->_right, os, indent);
            }
                break;
            case base_t_t::MATH_OP:
            {
                std::shared_ptr<const MathOpExpr> mathPtr = std::static_pointer_cast<const MathOpExpr>(root);
                os << math2str[int(mathPtr->math_t_)] << std::endl;
                _exprOutputVisit(mathPtr->_left, os, indent);
                _exprOutputVisit(mathPtr->_right, os, indent);
            }
                break;
            case base_t_t::ID:
            {
                std::shared_ptr<const IdExpr> idPtr = std::static_pointer_cast<const IdExpr>(root);
                os << idPtr->_tableName << "." << idPtr->_columnName << std::endl;
            }
                break;
            case base_t_t::NUMERIC:
            {
                std::shared_ptr<const NumericExpr> numericPtr = std::static_pointer_cast<const NumericExpr>(root);
                os << numericPtr->_value << std::endl;
            }
                break;
            case base_t_t::STR:
            {
                std::shared_ptr<const StrExpr> strPtr = std::static_pointer_cast<const StrExpr>(root);
                os << strPtr->_value << std::endl;
            }
                break;
            default:
                os << "Unexpected base_t" << std::endl;
                break;
        }
    }

    void exprOutputVisit(std::shared_ptr<const BaseExpr> root, std::ostream& os)
    {
        if(root)
            _exprOutputVisit(root, os, 2);
    }

}

