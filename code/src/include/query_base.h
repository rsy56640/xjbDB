//
// Created by 刘瑞康 on 2020/2/17.
//

#pragma once

#include <iostream>
#include <string>
#include <utility>

/*
 * this file includes
 *  commom definitions for tp and ap
 */

namespace DB::util
{
    template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
    template<class... Ts> overloaded(Ts...)->overloaded<Ts...>;
}


namespace DB::query {

    struct Exit
    {
        void print() const
        {
            std::cout << "Exit" << std::endl;

        }
    };

    // any unexpected cases will be catched and converted into ErrorMsg for vm
    struct ErrorMsg {
        std::string _msg;

        ErrorMsg(std::string msg):_msg(std::move(msg)) {}

        void print() const
        {
            std::cout << _msg << std::endl;
        }
    };

    // switch mode between tp and ap
    struct Switch {
        void print() const
        {
            std::cout << "Switch" << std::endl;
        }
    };

    // show system info
    struct Show
    {
        void print() const
        {
            std::cout << "Show" << std::endl;
        }
    };

    struct Schema
    {
        void print() const
        {
            std::cout << "Schema" << std::endl;
        }
    };

}	//end namespace DB::query