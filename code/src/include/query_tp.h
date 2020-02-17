#pragma once

#include "table.h"
#include "query_base.h"
#include "ast_tp.h"
#include <vector>
#include <variant>
#include <optional>
#include <unordered_map>
#include <memory>

/*
 * this file includes
 *  specific definitions for tp
 */

namespace DB::query {


    //===========================================================
    //DDL
    struct CreateTableInfo
    {
        table::TableInfo tableInfo;
        std::vector<table::value_t> defaults;
        std::vector<std::string> fkTables;
        void print() const
        {
            std::cout << "Create table : " << tableInfo.tableName_ << std::endl;
        }
    };

    struct DropTableInfo
    {
        std::string tableName;
        void print() const
        {
            std::cout << "Drop table : " << tableName << std::endl;
        }
    };


    //===========================================================
    //DML

    struct Element {
        std::string name;
        std::shared_ptr<ast::AtomExpr> valueExpr;
    };

    using Elements = std::vector<Element>;

    struct InsertInfo {
        std::string sourceTable;
        Elements elements;
        void print() const
        {
            std::cout << "Insert into table : " << sourceTable << std::endl;
            for (const Element &ele : elements)
            {
                std::cout << "Column : " << ele.name << std::endl;
                ast::outputVisit(ele.valueExpr, std::cout);
            }
        }
    };

    struct UpdateInfo {
        std::string sourceTable;
        Elements elements;
        std::shared_ptr<ast::BaseExpr> whereExpr;
        void print() const
        {
            std::cout << "Update table : " << sourceTable << std::endl;
            for (const Element &ele : elements)
            {
                std::cout << "Column : " << ele.name << std::endl;
                ast::outputVisit(ele.valueExpr, std::cout);
            }
			if (whereExpr)
			{
				std::cout << "Where Expression:" << std::endl;
				ast::outputVisit(whereExpr, std::cout);
			}
        }
    };

    struct DeleteInfo {
        std::string sourceTable;
        std::shared_ptr<ast::BaseExpr> whereExpr;
        void print() const
        {
            std::cout << "Delete from table : " << sourceTable << std::endl;
			if (whereExpr)
			{
				std::cout << "Where Expression:" << std::endl;
				ast::outputVisit(whereExpr, std::cout);
			}
        }
    };

    using OrderbyElement = std::pair<ast::BaseExpr*, bool>;	//	orderExpr, isASC

    struct TPSelectInfo {
        std::shared_ptr<ast::BaseOp> opRoot;
        std::vector<OrderbyElement> orderbys;
        void print() const
        {
            std::cout << "Select : " << std::endl;
            ast::outputVisit(opRoot, std::cout);
        }
    };


    struct Show
    {
        void print() const
        {
            std::cout << "Show" << std::endl;

        }
    };


    //tp query return type to vm
    using TPValue = std::variant<CreateTableInfo, DropTableInfo, TPSelectInfo, UpdateInfo, InsertInfo, DeleteInfo, Show, Exit, ErrorMsg, Switch>;

    void print(const TPValue &value);

    TPValue tp_parse(const std::string &sql);

}	//end namespace DB::query
