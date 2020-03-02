//
// Created by 刘瑞康 on 2020/2/17.
//

#pragma once

#include "query_base.h"
#include "table.h"
#include "ast_ap.h"
#include "ap_exec.h"
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


    class APSelectInfo {
    public:
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

        void generateCode();

        void compile();

        void load();

        void close();

        ap::VMEmitOp query(const ap::ap_table_array_t& tables) const;

        const table::schema_t& get_schema() const { return schema; }

    public:
        vector<string> tables;
        vector<shared_ptr<ast::BaseExpr>> conditions;
        vector<std::pair<string, string>> columns; // selected pairs of<table, column>

    private:

        typedef ap::VMEmitOp (*QUERY_PTR)(const ap::ap_table_array_t& tables);

        QUERY_PTR _query_;
        void *_handle;

        void set_schema(ast::APMap map);
        table::schema_t schema;

    };

    using APValue = std::variant<APSelectInfo, Exit, ErrorMsg, Switch>;

    void print(const APValue &value);

    APValue ap_parse(const std::string &sql);

}	//end namespace DB::query
