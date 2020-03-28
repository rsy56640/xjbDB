#pragma once
#include <chrono>
#include <cstdio>
#include "debug_log.h"

namespace DB {

    using time_point_t = std::chrono::time_point<std::chrono::system_clock>;
    static inline
    void print_timing(const char* task,time_point_t begin, time_point_t end) {
        if(debug::TIMING) {
            std::chrono::duration<double> diff = end - begin;
            printf("[time] used for [%s]: %lfms\n", task, diff.count() * 1000);
        }
    }

} // end namespace DB