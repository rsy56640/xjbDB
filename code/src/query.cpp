#pragma once
#include <iostream>
#include "include/query.h"
#include "include/lexer.h"
#include "include/parse.h"

namespace DB::util
{
	template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
	template<class... Ts> overloaded(Ts...)->overloaded<Ts...>;
}

namespace DB::query {

	void print(const SQLValue &value)
	{
		std::cout << "=========Print SQLValue============================" << std::endl;
		std::visit(DB::util::overloaded{
			[](const CreateTableInfo& t) {},
			[](const DropTableInfo& t) { t.print(); },
			[](const SelectInfo& t) { t.print(); },
			[](const UpdateInfo& t) { t.print(); },
			[](const InsertInfo& t) { t.print(); },
			[](const DeleteInfo& t) { t.print(); },
			[](const Exit& t) { t.print(); },
			[](const ErrorMsg& t) { t.print(); }
			}, value);
		std::cout << "=========End SQLValue============================" << std::endl;
	}

	SQLValue sql_parse(const std::string &sql)
	{
		try
		{
			DB::lexer::Lexer lexer;
			std::cout << "\n--Start Tokenize---------------------------------------\n" << std::endl;
			lexer.tokenize(sql.c_str(), sql.size());
			lexer.print(std::cout);
			std::cout << "\n--End Tokenize---------------------------------------\n" << std::endl;
			std::cout << "\n--Start Parse---------------------------------------\n" << std::endl;
			DB::query::SQLValue value = analyze(lexer.getTokens()).sqlValue;
			std::cout << "\n--End Parse---------------------------------------\n" << std::endl;
			return value;
		}
		catch (const DB::DB_Base_Exception& e)
		{
			e.printException(); std::cout << std::endl;
			return ErrorMsg(e.str());	//change to details
		}
		catch (const std::string &e)
		{
			std::cout << e << std::endl;
			return ErrorMsg(e);
		}
		catch (const std::exception& e) { std::cout << e.what() << std::endl << std::endl; }
		catch (...) { std::cout << "Unexpected Exception" << std::endl << std::endl; }
		return ErrorMsg("unexpected exception");
	}

} //end namespace DB
