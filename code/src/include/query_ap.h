//
// Created by 刘瑞康 on 2020/2/17.
//

#pragma once

#include "query_base.h"
#include "ast_ap.h"
#include <variant>
#include <vector>

using std::string;
using std::vector;


/*
 * this file includes
 *  specific definitions for ap
 */

namespace DB::query {

    struct APSelectInfo {
        //currently suppose select all
        //vector< std::pair<string, string> > columns; // selected pairs of<table, column>

        vector<string> tables;
        vector<ast::BaseExpr*> conditions;  //shared_ptr?

        void print() const
        {

        }
    };

    using APValue = std::variant<APSelectInfo, Exit, ErrorMsg, Switch>;

    void print(const APValue &value);

    APValue ap_parse(const std::string &sql);

}	//end namespace DB::query
