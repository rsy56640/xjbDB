//
// Created by 刘瑞康 on 2020/2/19.
//

#include "ast_ap.h"

namespace DB::ast{

    //expression type
    enum class ap_check_t {INT, STR, BOOL};

    ap_check_t _apCheckSingle(shared_ptr<const BaseExpr> condition)
    {
        base_t_t base_t = condition->base_t_;

        switch (base_t)
        {
            case base_t_t::COMPARISON_OP:{
                auto comparisonPtr = std::static_pointer_cast<const ComparisonOpExpr>(condition);
                ap_check_t left_t = _apCheckSingle(comparisonPtr->_left);
                ap_check_t right_t = _apCheckSingle(comparisonPtr->_right);
                if(left_t != right_t)
                {
                    break;
                }
                return ap_check_t::BOOL;
            }
            case base_t_t::MATH_OP:{
                auto mathPtr = std::static_pointer_cast<const MathOpExpr>(condition);
                auto &left = mathPtr->_left;
                auto &right = mathPtr->_right;
                ap_check_t left_t = _apCheckSingle(left);
                ap_check_t right_t = _apCheckSingle(right);
                if(left_t != right_t)
                {
                    break;
                }

                // if both constant, merge
                if(left->base_t_ == base_t_t::NUMERIC && right->base_t_ == base_t_t::NUMERIC)
                {
                    auto leftNum = std::static_pointer_cast<const NumericExpr>(left);
                    auto rightNum = std::static_pointer_cast<const NumericExpr>(right);
                    int res = numericOp(leftNum->_value, rightNum->_value, mathPtr->math_t_);
                    //condition = std::make_shared<NumericExpr>(res);
                }
                else if(left->base_t_ == base_t_t::STR && right->base_t_ == base_t_t::STR)
                {
                    if(mathPtr->math_t_ != math_t_t::ADD)
                        throw string("STR can only be connected with '+'");
                    auto leftStr = std::static_pointer_cast<const StrExpr>(left);
                    auto rightStr = std::static_pointer_cast<const StrExpr>(right);
                    //condition = std::make_shared<StrExpr>(leftStr->_value + rightStr->_value);
                }

                return left_t;
            }
            case base_t_t::NUMERIC:{
                return ap_check_t::INT;
            }
            case base_t_t::STR:{
                return ap_check_t::STR;
            }
            case base_t_t::ID:{
                //TODO
                return ap_check_t::BOOL;
            }
            default:{
                throw string("unexpected expression type in condition : " + base2str[int(base_t)]);
            }
        }

        //matching cases shouldn't reach here
        throw string("expression types don't match");

    }

    void apCheckSingle(shared_ptr<const BaseExpr> condition)
    {
#ifdef DEBUG
        if (!condition)
            throw string("where condition is nullptr");
#endif // DEBUG

        if(condition->base_t_ != base_t_t::COMPARISON_OP)
        {
            throw string("condition type invalid, must be a comparison");
        }

        _apCheckSingle(condition);
    }

    void apCheckVisit(vector<shared_ptr<const BaseExpr>> &conditions)
    {
        for(const auto& condition : conditions)
        {
            apCheckSingle(condition);
        }
    }

}