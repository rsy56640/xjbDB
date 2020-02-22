//
// Created by 刘瑞康 on 2020/2/19.
//

#include "ast_ap.h"
#include "table.h"
#include "page.h"

namespace DB::ast{

    // op node
    APBaseOp::~APBaseOp() {}

    APTableOp::APTableOp(const string tableName) : APBaseOp(ap_op_t_t::TABLE)
    {
        // throw exception if not exist
    }

    //expression type
    enum class ap_check_t {INT, STR, BOOL};

    ap_check_t _apCheckSingle(shared_ptr<BaseExpr> &expr)
    {
        base_t_t base_t = expr->base_t_;

        switch (base_t)
        {
            case base_t_t::COMPARISON_OP:{
                auto comparisonPtr = std::static_pointer_cast<ComparisonOpExpr>(expr);
                ap_check_t left_t = _apCheckSingle(comparisonPtr->_left);
                ap_check_t right_t = _apCheckSingle(comparisonPtr->_right);
                if(left_t != right_t)
                {
                    break;
                }
                return ap_check_t::BOOL;
            }
            case base_t_t::MATH_OP:{
                auto mathPtr = std::static_pointer_cast<MathOpExpr>(expr);
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
                    expr = std::make_shared<NumericExpr>(res);
                }
                else if(left->base_t_ == base_t_t::STR && right->base_t_ == base_t_t::STR)
                {
                    if(mathPtr->math_t_ != math_t_t::ADD)
                        throw string("STR can only be connected with '+'");
                    auto leftStr = std::static_pointer_cast<const StrExpr>(left);
                    auto rightStr = std::static_pointer_cast<const StrExpr>(right);
                    expr = std::make_shared<StrExpr>(leftStr->_value + rightStr->_value);
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
                std::shared_ptr<const IdExpr> idPtr = std::static_pointer_cast<const IdExpr>(expr);
                page::col_t_t id_t;
                id_t = table::getColumnInfo(idPtr->_tableName, idPtr->_columnName).col_t_;
                if (id_t == page::col_t_t::INTEGER)
                    return ap_check_t::INT;
                else if (id_t == page::col_t_t::CHAR || id_t == page::col_t_t::VARCHAR)
                    return ap_check_t::STR;
            }
            default:{
                throw string("unexpected expression type in condition : " + base2str[int(base_t)]);
            }
        }

        //matching cases shouldn't reach here
        throw string("expression types don't match");

    }

    void apCheckSingle(shared_ptr<BaseExpr> &condition)
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

    void apCheckVisit(vector<shared_ptr<BaseExpr>> &conditions)
    {
        for(auto& condition : conditions)
        {
            apCheckSingle(condition);
        }
    }


    shared_ptr<APEmitOp> generateAst(const vector<string> &tables, const vector<shared_ptr<BaseExpr>> &conditions, vector<Column> columns)
    {
        vector<int> singleConditions;   // only one table included
        vector<int> joinConditions;   // currently consider only T1.v == T2.v
        vector<int> complexConditions;   // other conditions

        int hashTableIndex = 0;
        map<string, APBaseOp*> tableDict;  // table name -> Op containing the table
        map<int, set<string>> condDict; // condition index -> tables involved

        for(const auto &table : tables)
        {
            tableDict[table] = new APTableOp(table);
        }

        for(int i = 0; i < conditions.size(); ++i)
        {
            auto tableSet = conditions[i]->getTables();
            condDict[i] = tableSet;
            if(tableSet.size() == 1)
                singleConditions.push_back(i);
            else if(tableSet.size() == 2 && conditions[i]->isJoin())
                joinConditions.push_back(i);
            else
                complexConditions.push_back(i);
        }

        for(int condIndex : singleConditions)
        {
            if (condDict[condIndex].size() != 1)
                throw string("wrong condition table set");

            //package specific table into FilterOP
            const string &table = *condDict[condIndex].begin();
            APBaseOp *filterOp = new APFilterOp(tableDict[table], conditions[condIndex]);
            tableDict[table]->setParentOp(filterOp);

            //replace dict
            tableDict[table] = filterOp;
        }

        for(auto condIndex : joinConditions)
        {
            if (condDict[condIndex].size() != 2)
                throw string("wrong condition table set");

            //package tables into JoinOP
            auto it = condDict[condIndex].begin();
            const string &table1 = *it, &table2 = *++it;
            APBaseOp *joinOp = new APJoinOp(tableDict[table1], tableDict[table2], conditions[condIndex], hashTableIndex++);
            tableDict[table1]->setParentOp(joinOp);
            tableDict[table2]->setParentOp(joinOp);

            //replace dict
            tableDict[table1] = joinOp;
            tableDict[table2] = joinOp;
        }

        for(auto condition : complexConditions)
        {
            // currently don't complex condition

            //package specific table into Filter

            //replace dict
        }

        // currently don't handle projection

        APBaseOp *merge = tableDict.begin()->second;
        for(const auto &kv : tableDict)
        {
            if(kv.second != merge)
                throw string("generate ast error, exist unjoined tables");
        }

        //package into EmitOP
        shared_ptr<APEmitOp> emit{new APEmitOp(merge, hashTableIndex)};
        merge->setParentOp(emit.get());
        return emit;
    }

}