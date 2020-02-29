//
// Created by 刘瑞康 on 2020/2/19.
//

#include "ast_ap.h"
#include "table.h"
#include "page.h"
#include "vm.h"
#include <string>
#include <map>
#include <unordered_map>
#include <utility>

#define START_BASE_LINE 7

namespace DB::ast{

    using std::to_string;
    using std::string;
    using std::map;
    using std::unordered_map;

    int g_iTableCount, g_iHashCount, g_iIndent;
    vector<string> g_vCode = {};

    static inline
    std::string range2str(page::range_t range) {
        std::string str = "{ ";
        str += std::to_string(range.begin);
        str += ", ";
        str += std::to_string(range.len);
        str += " }";
        return str;
    }

    // init from source table
    APMap::APMap(const table::TableInfo& table)
        :attr_map(), tuple_len(table.columnInfos_.back().get_range().end())
    {
        const std::string tableName = table.tableName_;
        const uint32_t attr_size = table.colNames_.size();
        for(int i = 0; i < attr_size; i++) {
            attr_map[{ tableName, table.colNames_[i] }] =
                table.columnInfos_[i].get_range();
        }
        if(table.hasPK()) {
            unique_ranges_.insert(table.columnInfos_[table.pk_col_].get_range());
        }
    }

    void APMap::join(const APMap& right, page::range_t left_range, page::range_t right_range) {
        const uint32_t offset = tuple_len;
        tuple_len += right.len();
        for(auto const& [col_name, range]: right.attr_map) {
            attr_map[col_name] = page::range_t{ range.begin + offset, range.len };
        }

        APMap& left = *this;
        std::unordered_set<page::range_t> unique_ranges{};
        const bool left_join_key_unique = left.check_unique(left_range);
        const bool right_join_key_unique = right.check_unique(right_range);
        if(!left_join_key_unique && !right_join_key_unique) { // NULL
            unique_ranges.clear();
        }
        else if(!left_join_key_unique && right_join_key_unique) { // left unique ranges
            unique_ranges = std::move(left.unique_ranges_);
        }
        else if(left_join_key_unique && !right_join_key_unique) { // right unique ranges
            for(page::range_t range : right.unique_ranges_) {
                unique_ranges.insert({ range.begin + offset, range.len });
            }
        }
        else { // all unique ranges
            unique_ranges = std::move(left.unique_ranges_);
            for(page::range_t range : right.unique_ranges_) {
                unique_ranges.insert({ range.begin + offset, range.len });
            }
        }

        this->unique_ranges_ = std::move(unique_ranges);
    }

    uint32_t APMap::len() const { return tuple_len; }

    bool APMap::check_unique(page::range_t range) const { return unique_ranges_.count(range); }

    page::range_t APMap::get(const col_name_t& attr) {
        return attr_map[attr];
    }


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
         * ……                   BASE
         * VMEmitOp emit;       BASE
         * const ap_table_t&    _tableCount
         * range_t              _hashTableCount * 2
         * hash_table_t         _hashTableCount
         */
        g_vCode.resize(START_BASE_LINE + _tableCount + 3 * _hashTableCount);
        g_vCode[0] = "// this file is generated ";
        g_vCode[1] = "#include \"ap_exec.h\"";
        g_vCode[2] = "namespace DB::ap {";
        g_vCode[3] = "static block_tuple_t projection(const block_tuple_t& block) { return block; }";
        g_vCode[4] = "extern";
        g_vCode[5] = "VMEmitOp query(const ap_table_array_t& tables) {";
        g_vCode[6] = "VMEmitOp emit;";

        _table->produce();
    }

    void APEmitOp::consume(APBaseOp *source, APMap &map)
    {
        g_vCode.push_back("emit.emit(block);");

        while(g_iIndent > 0)
        {
            g_vCode.push_back("}");
            g_iIndent--;
        }

        g_vCode.push_back("return emit;");
        g_vCode.push_back("} // end codegen function");
    }


    void APFilterOp::produce()
    {
        _table->produce();
    }

    string generateCondStr(shared_ptr<BaseExpr> condition, APMap &map)
    {
        // generate condition string with map

        base_t_t base_t = condition->base_t_;
        switch (base_t)
        {
            case base_t_t::COMPARISON_OP:
            {
                std::shared_ptr<const ComparisonOpExpr> comparisonPtr = std::static_pointer_cast<const ComparisonOpExpr>(condition);
                string strLeft = generateCondStr(comparisonPtr->_left, map);
                string strRight = generateCondStr(comparisonPtr->_right, map);
                return strLeft + comparison2str[int(comparisonPtr->comparison_t_)] + strRight;
            }
            case base_t_t::MATH_OP:
            {
                std::shared_ptr<const MathOpExpr> mathPtr = std::static_pointer_cast<const MathOpExpr>(condition);
                string strLeft = generateCondStr(mathPtr->_left, map);
                string strRight = generateCondStr(mathPtr->_right, map);
                return strLeft + math2str[int(mathPtr->math_t_)] + strRight;
            }
            case base_t_t::ID:
            {
                std::shared_ptr<const IdExpr> idPtr = std::static_pointer_cast<const IdExpr>(condition);

                page::range_t range = map.get({ idPtr->_tableName, idPtr->_columnName });
                std::string id_name = " block.getINT(" + range2str(range) + ") ";
                return id_name;
            }
            case base_t_t::NUMERIC:
            {
                std::shared_ptr<const NumericExpr> numericPtr = std::static_pointer_cast<const NumericExpr>(condition);
                return to_string(numericPtr->_value);
            }
            case base_t_t::STR:
            {
                std::shared_ptr<const StrExpr> strPtr = std::static_pointer_cast<const StrExpr>(condition);
                return strPtr->_value;
            }
        }

        // unexpect to reach here
        throw string("unexpected bast_t in generateCondStr");
    }

    void APFilterOp::consume(APBaseOp *source, APMap &map)
    {
        g_vCode.push_back("block.selectivity_and(" + generateCondStr(_condition, map) + ");");

        // map doesn't need change
        _parentOp->consume(this, map);
    }

    void APJoinOp::produce()
    {
        string strIndex = to_string(_hashTableIndex);

        // reserved lines for hash table declaration
        g_vCode[START_BASE_LINE + g_iTableCount + g_iHashCount * 2 + _hashTableIndex] =
                "hash_table_t ht" + strIndex +
                "(rngLeft" + strIndex + ",rngRight" + strIndex + ",";

        _tableLeft->produce();
        _tableRight->produce();
    }

    void APJoinOp::consume(APBaseOp *source, APMap &map)
    {
        string strIndex = to_string(_hashTableIndex);

        g_vCode[START_BASE_LINE + g_iTableCount + g_iHashCount * 2 + _hashTableIndex]
                += to_string(map.len()) + ",";

        if(source == _tableLeft)
        {
            _leftMap = map;

            _leftRange = map.get(_leftAttr);
            isUnique = map.check_unique(_leftRange);

            // reserved lines for left child rng declaration
            g_vCode[START_BASE_LINE + g_iTableCount + _hashTableIndex * 2] =
                    "range_t rngLeft" + strIndex +
                    range2str(_leftRange) + ";";

            g_vCode.push_back("ht" + strIndex + ".insert(block);");

            while(g_iIndent > 0)
            {
                g_vCode.push_back("}");
                g_iIndent--;
            }

            g_vCode.push_back("ht" + strIndex + ".build();");
        }
        else if(source == _tableRight)
        {
            g_iIndent++;

            g_vCode[START_BASE_LINE + g_iTableCount + g_iHashCount * 2 + _hashTableIndex]
                    += isUnique?"true);":"false);";

            page::range_t right_range = map.get(_rightAttr);
            // reserved lines for right child rng declaration
            g_vCode[START_BASE_LINE + g_iTableCount + _hashTableIndex * 2 + 1] =
                    "range_t rngRight" + strIndex +
                    range2str(right_range) + ";";

            // main content
            g_vCode.push_back("join_result_buf_t join_result" + strIndex + " = ht" + strIndex + ".probe(block);");
            g_vCode.push_back("for(ap_block_iter_t it = join_result" + strIndex + ".get_block_iter(); !it.is_end();) {");
            g_vCode.push_back("block_tuple_t block = it.consume_block();");

            _leftMap.join(map, _leftRange, right_range);
            _parentOp->consume(this, _leftMap);
        }
        else
        {
            throw string("unexpected joined source in APJoinOp::consume");
        }
    }

    APTableOp::APTableOp(const table::TableInfo& table, string tableName, int tableIndex)
        :APBaseOp(ap_op_t_t::TABLE), _tableName(tableName), _tableIndex(tableIndex), _map(table) {}

    void APTableOp::produce()
    {
        g_iIndent++;

        string strIndex = to_string(_tableIndex);

        // reserved lines for table declaration
        g_vCode[START_BASE_LINE + _tableIndex] =
                "const ap_table_t& T" + strIndex +
                " = tables.at(" + std::to_string(table::vm_->get_ap_table_index(_tableName)) + ");";

        g_vCode.push_back("for(ap_block_iter_t it = T" + strIndex + ".get_block_iter();" +
                " !it" + strIndex + ".is_end();) {");

        g_vCode.push_back("block_tuple_t block = it.consume_block();");

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

        for(const auto &tableName : tables)
        {
            // tableName -> TableInfo, throw exception if not exist
            table::TableInfo tableinfo = table::getTableInfo(tableName);
            tableDict[tableName] = new APTableOp(tableinfo, tableName, tableIndex++);
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
            auto joinCond = conditions[condIndex];
            shared_ptr<const ComparisonOpExpr> comparisonPtr = std::static_pointer_cast<const ComparisonOpExpr>(joinCond);
            shared_ptr<const IdExpr> leftPtr = std::static_pointer_cast<const IdExpr>(comparisonPtr->_left);
            shared_ptr<const IdExpr> rightPtr = std::static_pointer_cast<const IdExpr>(comparisonPtr->_right);

            // is only one is JoinOp, swap it to the right child
            if(tableDict[leftPtr->_tableName]->isJoin())
                leftPtr.swap(rightPtr);

            string tableLeft = leftPtr->_tableName, tableRight = rightPtr->_tableName;
            col_name_t leftCol = std::make_pair(tableLeft, leftPtr->_columnName);
            col_name_t rightCol = std::make_pair(tableRight, rightPtr->_columnName);
            APBaseOp *joinOp = new APJoinOp(tableDict[tableLeft], tableDict[tableRight], leftCol, rightCol, hashTableIndex++);
            tableDict[tableLeft]->setParentOp(joinOp);
            tableDict[tableRight]->setParentOp(joinOp);

            //replace dict
            APBaseOp *oldLeft = tableDict[tableLeft];
            APBaseOp *oldRight = tableDict[tableRight];
            for(auto &kv : tableDict)
            {
                if(kv.second == oldLeft || kv.second == oldRight)
                    kv.second = joinOp;
            }
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




    vector<string> generateCode(shared_ptr<APEmitOp> emit)
    {
        emit->produce();

        return g_vCode;
    }
}