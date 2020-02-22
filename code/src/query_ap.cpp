//
// Created by 刘瑞康 on 2020/2/17.
//

#include "query_ap.h"
#include "include/lexer.h"
#include "include/debug_log.h"
#include "parse_ap.h"


namespace DB::query{

    void print(const APValue &value)
    {
        std::cout << "=========Print APValue============================" << std::endl;
        std::visit(DB::util::overloaded{
                [](const APSelectInfo& t) { t.print(); },
                [](const Exit& t) { t.print(); },
                [](const ErrorMsg& t) { t.print(); },
                [](const Switch& t) { t.print(); },
                [](auto&&) { debug::ERROR_LOG("`print(APValue)`\n"); },
        }, value);
        std::cout << "=========End APValue============================" << std::endl;
    }

    APValue ap_parse(const std::string &sql)
    {
        APValue value;
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

            auto res = parse_ap::analyze(lexer.getTokens());
            value = res.apValue;

            if (debug::PARSE_LOG)
                std::cout << "\n--End Parse---------------------------------------\n" << std::endl;

            //for APSelectInfo, check & optimize before return
            if (auto ptr = get_if<APSelectInfo>(&value))
            {
                apCheckVisit(ptr->conditions);
                auto emit = generateAst(ptr->tables, ptr->conditions);
            }
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
}