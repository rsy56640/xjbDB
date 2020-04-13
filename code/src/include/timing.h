#pragma once
#include <chrono>
#include <cstdio>
#include "debug_log.h"

namespace DB {

    using time_point_t = std::chrono::time_point<std::chrono::system_clock>;
    static inline
    void print_timing(time_point_t begin, time_point_t end, const char* task) {
        if(debug::TIMING) {
            std::chrono::duration<double> diff = end - begin;
            std::printf("[time] used for [%s]: %lfms\n", task, diff.count() * 1000);
        }
    }

    template<typename ...Args>
    void print_timing(time_point_t begin, time_point_t end, const char* task_format, Args ...args) {
        if(debug::TIMING) {
            std::chrono::duration<double> diff = end - begin;
            char buffer_format[128] = { 0 };
            std::sprintf(buffer_format, "[time] used for [%s]: %%lfms\n", task_format);
            std::printf(buffer_format, args..., diff.count() * 1000);
        }
    }

} // end namespace DB