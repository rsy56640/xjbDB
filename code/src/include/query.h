#pragma once
#include <iostream>
#include <string>
#include <vector>
#include <variant>
#include <optional>
#include <unordered_map>
#include "table.h"
#include "ast.h"

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
		ast::AtomExpr* valueExpr;
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
		ast::BaseExpr* whereExpr;
		void print() const
		{
			std::cout << "Update table : " << sourceTable << std::endl;
			for (const Element &ele : elements)
			{
				std::cout << "Column : " << ele.name << std::endl;
				ast::outputVisit(ele.valueExpr, std::cout);
			}
			std::cout << "Where Expression:" << std::endl;
			ast::outputVisit(whereExpr, std::cout);
		}
	};

	struct DeleteInfo {
		std::string sourceTable;
		ast::BaseExpr* whereExpr;
		void print() const
		{
			std::cout << "Delete from table : " << sourceTable << std::endl;
			std::cout << "Where Expression:" << std::endl;
			ast::outputVisit(whereExpr, std::cout);
		}
	};
	
	using OrderbyElement = std::pair<ast::BaseExpr*, bool>;	//	orderExpr, isASC
	struct SelectInfo {
		ast::BaseOp* opRoot;
		std::vector<OrderbyElement> orderbys;
		void print() const
		{
			std::cout << "Select : " << std::endl;
			ast::outputVisit(opRoot, std::cout);
		}
	};


	//===========================================================
	//specified for vm

	struct Exit
	{
		void print() const
		{
			std::cout << "Exit" << std::endl;

		}
	};

	struct ErrorMsg {
		std::string _msg;
		
		ErrorMsg(const std::string msg)
		{
			_msg = msg;
		}

		void print() const
		{
			std::cout << _msg << std::endl;
		}
	};

	//return type to vm, any exception that occurs will be catched and converted into ErrorMsg
	using SQLValue = std::variant < CreateTableInfo, DropTableInfo, SelectInfo, UpdateInfo, InsertInfo, DeleteInfo, Exit, ErrorMsg>;

	void print(const SQLValue &value);
	
	SQLValue sql_parse(const std::string &sql);

}	//end namespace DB::query
