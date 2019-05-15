#pragma once
#include <iostream>
#include "include/query.h"
#include "include/lexer.h"
#include "include/parse.h"
#include "include/debug_log.h"

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
			[](const Show& t) { t.print(); },
			[](const Exit& t) { t.print(); },
			[](const ErrorMsg& t) { t.print(); },
            [](auto&&) { debug::ERROR_LOG("`print(SQLValue)`\n"); },
			}, value);
		std::cout << "=========End SQLValue============================" << std::endl;
	}

	SQLValue sql_parse(const std::string &sql)
	{
		SQLValue value;
		try
		{
			DB::lexer::Lexer lexer;
			if (debug::LEXER_LOG)
				std::cout << "\n--Start Tokenize---------------------------------------\n" << std::endl;

			lexer.tokenize(sql.c_str(), sql.size());

			if(debug::LEXER_LOG)
				lexer.print(std::cout);

			if (debug::LEXER_LOG)
			{
				std::cout << "\n--End Tokenize---------------------------------------\n" << std::endl;
			}
			if (debug::PARSE_LOG)
			{
				std::cout << "\n--Start Parse---------------------------------------\n" << std::endl;
			}

			auto res = analyze(lexer.getTokens());
			value = res.sqlValue;

			if (debug::PARSE_TREE_PNG)
			{
				auto root = res.stk.top();
				std::fstream fs("ast.dot", std::fstream::out);
				DotGen(root, fs);
				fs.close();
			}

			if (debug::PARSE_LOG)
				std::cout << "\n--End Parse---------------------------------------\n" << std::endl;
		}
		catch (const DB::DB_Base_Exception& e)
		{
			e.printException(); std::cout << std::endl;
			value = ErrorMsg(e.str());	//change to details
		}
		catch (const std::string &e)
		{
			std::cout << e << std::endl;
			value = ErrorMsg(e);
		}
		catch (...) { value = ErrorMsg("unexpected exception"); }

		if (debug::QUERY_LOG)
			print(value);

		return value;
	}

} //end namespace DB
