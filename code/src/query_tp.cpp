#pragma once
#include <iostream>
#include "query_tp.h"
#include "include/lexer.h"
#include "parse_tp.h"
#include "include/debug_log.h"


namespace DB::query {

	void print(const TPValue &value)
	{
		std::cout << "=========Print TPValue============================" << std::endl;
		std::visit(DB::util::overloaded{
			[](const CreateTableInfo& t) { t.print(); },
			[](const DropTableInfo& t) { t.print(); },
			[](const TPSelectInfo& t) { t.print(); },
			[](const UpdateInfo& t) { t.print(); },
			[](const InsertInfo& t) { t.print(); },
			[](const DeleteInfo& t) { t.print(); },
			[](const Show& t) { t.print(); },
			[](const Exit& t) { t.print(); },
			[](const ErrorMsg& t) { t.print(); },
			[](const Switch& t) { t.print(); },
			[](auto&&) { debug::ERROR_LOG("`print(TPValue)`\n"); },
			}, value);
		std::cout << "=========End TPValue============================" << std::endl;
	}

	TPValue tp_parse(const std::string &sql)
	{
		TPValue value;
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

			auto res = parse_tp::analyze(lexer.getTokens());
			value = res.tpValue;


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
