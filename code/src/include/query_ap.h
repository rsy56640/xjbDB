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
using std::shared_ptr;


/*
 * this file includes
 *  specific definitions for ap
 */

namespace DB::query {


    struct APSelectInfo {

        vector<string> tables;
        vector<shared_ptr<ast::BaseExpr>> conditions;  //shared_ptr?

        //currently suppose select all
        //vector< std::pair<string, string> > columns; // selected pairs of<table, column>

        void print() const
        {
            std::cout << "APSelectInfo : " << std::endl;
            std::cout << "tables : ";
            for(auto &table : tables)
            {
                std::cout << table << " ";
            }
            std::cout << std::endl << "conditions : " << std::endl;
            for(const auto& condition : conditions)
            {
                ast::exprOutputVisit(condition, std::cout);
            }
        }
    };

    using APValue = std::variant<APSelectInfo, Exit, ErrorMsg, Switch>;

    void print(const APValue &value);

    APValue ap_parse(const std::string &sql);

}	//end namespace DB::query
