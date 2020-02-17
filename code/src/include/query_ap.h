//
// Created by 刘瑞康 on 2020/2/17.
//

#pragma once

#include "query_base.h"
#include <variant>
#include <vector>

using std::string;
using std::vector;


/*
 * this file includes
 *  specific definitions for ap
 */

namespace DB::query {

    //reuse ast_tp -> ast_base?
    // column op column, column op constant
    struct SingleCondition{

    };

    struct APSelectInfo {
        //vector< std::pair<string, string> > columns; // <table, column>
        vector<string> tables;
        vector<SingleCondition> conditions;

        void print() const
        {

        }
    };

    using APValue = std::variant<APSelectInfo, Exit, ErrorMsg, Switch>;

    void print(const APValue &value);

    APValue ap_parse(const std::string &sql);

}	//end namespace DB::query
