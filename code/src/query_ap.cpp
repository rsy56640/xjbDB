//
// Created by 刘瑞康 on 2020/2/17.
//

#include "query_ap.h"
#include "include/lexer.h"
#include "include/debug_log.h"
#include "parse_ap.h"
#include <fstream>

namespace DB::query{

    void APSelectInfo::generateCode()
    {
        auto emit = generateAst(tables, conditions);
        auto code = ast::generateCode(emit);

        std::ofstream outfile;
        outfile.open("../../src/codegen/query.cpp");
        if(!outfile)
            return;
        for(const auto &line : code)
            outfile << line << endl;
        outfile.close();
    }

    void APSelectInfo::compile()
    {

    }

    ap::VMEmitOp APSelectInfo::query(const ap::ap_table_array_t& tables) const
    {
        return ap::VMEmitOp();
    }

    table::schema_t APSelectInfo::get_schema() const
    {
        return schema_t();
    }

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
                ptr->generateCode();
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