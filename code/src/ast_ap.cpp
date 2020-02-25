//
// Created by 刘瑞康 on 2020/2/19.
//

#include "ast_ap.h"
#include "table.h"
#include "page.h"

#define START_BASE_LINE 2

namespace DB::ast{

    using std::to_string;

    int g_iTableCount, g_iHashCount, g_iIndent;
    vector<string> g_vCode = {};

    // op node
    APBaseOp::~APBaseOp() {}

    void APEmitOp::produce()
    {
        // init
        g_vCode.clear();
        g_iTableCount = _tableCount;
        g_iHashCount = _hashTableCount;
        g_iIndent = 0;

        // fixed code + reserved lines
        /*
         * EmitOp query         BASE
         * VMEmitOp emit;       BASE
         * const ap_table_t*    _tableCount
         * range_t              _hashTableCount * 2
         * hash_table_t         _hashTableCount
         * probe_buf_t          _hashTableCount
         */
        g_vCode.resize(START_BASE_LINE + _tableCount + 4 * _hashTableCount);
        g_vCode[0] = "EmitOp query(connst table_container_t& tables) {";
        g_vCode[1] = "VMEmitOp emit;";

        _table->produce();
    }

    void APEmitOp::consume(APBaseOp *source, APMap &map)
    {
        g_vCode.push_back("return emit; }");
    }


    void APFilterOp::produce()
    {
        _table->produce();
    }

    string generateCondStr(shared_ptr<BaseExpr> condition, APMap &map)
    {
        // TODO: generate condition string with map

        return "";
    }

    void APFilterOp::consume(APBaseOp *source, APMap &map)
    {
        g_iIndent++;

        g_vCode.push_back("if(" + generateCondStr(_condition, map) + "){");

        // map doesn't need change
        _parentOp->consume(this, map);
    }

    void APJoinOp::produce()
    {
        string strIndex = to_string(_hashTableIndex);

        // reserved lines for hash table declaration
        g_vCode[START_BASE_LINE + g_iTableCount + g_iHashCount * 2 + _hashTableIndex] =
                "hash_table_t ht" + strIndex +
                "(rngLeft" + strIndex + ",rngRight" + strIndex + ");";

        g_vCode[START_BASE_LINE + g_iTableCount + g_iHashCount * 2 + _hashTableIndex + 1] =
                "probe_buf_t buf" + strIndex + ";";

        _tableLeft->produce();
        _tableRight->produce();
    }

    void APJoinOp::consume(APBaseOp *source, APMap &map)
    {
        string strIndex = to_string(_hashTableIndex);

        if(source == _tableLeft)
        {
            // reserved lines for left child rng declaration
            g_vCode[START_BASE_LINE + g_iTableCount + _hashTableIndex * 2] =
                    "range_t rngLeft" + strIndex +
                    "{" + "xxx" + "};"; //TODO: xxx -> (join condition + map) -> rng

            g_vCode.push_back("ht" + strIndex + ".insert(t);");

            while(g_iIndent > 0)
            {
                g_vCode.push_back("}");
                g_iIndent--;
            }

            g_vCode.push_back("ht" + strIndex + ".build();");
        }
        else if(source == _tableRight)
        {
            // reserved lines for right child rng declaration
            g_vCode[START_BASE_LINE + g_iTableCount + _hashTableIndex * 2 + 1] =
                    "range_t rngRight" + strIndex +
                    "{" + "xxx" + "};"; //TODO: xxx -> (join condition + map) -> rng

            // TODO: main content
            g_vCode.push_back("RIGHT_JOIN_CODE");

            // TODO: change map after join

            _parentOp->consume(this, map);
        }
        else
        {
            throw string("unexpected joined source in APJoinOp::consume");
        }
    }

    APTableOp::APTableOp(string tableName, int tableIndex)
        : APBaseOp(ap_op_t_t::TABLE), _tableIndex(tableIndex)
    {
        // throw exception if not exist

        // TODO: store the map of the table into _map
    }

    void APTableOp::produce()
    {
        g_iIndent++;

        string strIndex = to_string(_tableIndex);

        // reserved lines for table declaration
        g_vCode[START_BASE_LINE + _tableIndex] =
                "const ap_table_t* T" + strIndex +
                "tables.at(" + "xxx" + ");"; //TODO: xxx -> map of physical address

        g_vCode.push_back("for(ap_row_iter_t t = T" + strIndex + "->get_single_iter();" +
                " !t" + strIndex + ".is_end(); t" + strIndex + ".next()) {");

        _parentOp->consume(this, _map);
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

        int tableIndex = 0;
        int hashTableIndex = 0;
        map<string, APBaseOp*> tableDict;  // table name -> Op containing the table
        map<int, set<string>> condDict; // condition index -> tables involved

        for(const auto &table : tables)
        {
            tableDict[table] = new APTableOp(table, tableIndex++);
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
            string tableLeft = *it, tableRight = *++it;
            // is only one is JoinOp, swap it to the right child
            if(tableDict[tableLeft]->isJoin())
            {
                std::swap(tableLeft, tableRight);
            }
            APBaseOp *joinOp = new APJoinOp(tableDict[tableLeft], tableDict[tableRight], conditions[condIndex], hashTableIndex++);
            tableDict[tableLeft]->setParentOp(joinOp);
            tableDict[tableRight]->setParentOp(joinOp);

            //replace dict
            tableDict[tableLeft] = joinOp;
            tableDict[tableRight] = joinOp;
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
        shared_ptr<APEmitOp> emit{new APEmitOp(merge, hashTableIndex,tableIndex)};
        merge->setParentOp(emit.get());
        return emit;
    }




    string generateCode(shared_ptr<APEmitOp> emit)
    {
        emit->produce();

        // join the g_vCode strings
        string code;
        for(const auto &line : g_vCode)
        {
            code += line;
        }

        return code;
    }
}