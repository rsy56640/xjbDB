#pragma once
#include <string>
#include <cstdio>
#include <iostream>
#include <fstream>
#include "page.h"

namespace DB::debug
{

    constexpr bool
        PAGE_REF = false,
        LRU_EVICT = false,
        BT_CREATE = false,

        PAGE_READ = false,
        PAGE_WRITE = false,
        PAGE_GC = false,
        PAGE_FLUSH = false,         // (page) PAGE_FLUSH -> (buffer) BUFFER_FLUSH

        BUFFER_FETCH = false,
        BUFFER_FLUSH = false,       // (buffer) BUFFER_FLUSH -> (disk) PAGE_WRITE
        BUFFER_NEW = false,
        BUFFER_DELETE = false,

        SPLIT_ROOT = false,
        SPLIT_ROOT_INTERNAL = false,
        SPLIT_ROOT_LEAF = false,
        SPLIT_INTERNAL = false,
        SPLIT_LEAF = false,

        MERGE_INTERNAL = false,
        MERGE_LEAF = false,

        ERASE_ROOT_LEAF = false,
        ERASE_ROOT_INTERNAL = false,
        ERASE_NONMIN_LEAF = false,
        ERASE_NONMIN_INTERNAL = false,

        QUERY_PROCESS = false,

        PK_VIEW = false,

        WAL = false,
        RECOVERY = false,

        DESTROY_LOG = false,

        SQL_INPUT = false,
        LEXER_LOG = false,
        PARSE_LOG = false,
        QUERY_LOG = false,

        TP_QUERY_OUTPUT = false,
        AP_QUERY_OUTPUT = false,

        NON_DEBUG = false,
        xbjDB_DEBUG = true,

        AP_AST = true,
        AP_COMPILE = true,
        AP_DYNAMIC_LOAD = true,

        AP_EXEC = false,
        // build phase
        AP_EXEC_BUCKET_SIZE = false,
        AP_EXEC_HISTOGRAM = false,
        AP_EXEC_HASH_BUCKET = false,
        // probe phase
        AP_EXEC_PROBE_KEYS = false,
        AP_EXEC_POS = false,
        AP_EXEC_END_INCLUSIVE = false,
        AP_EXEC_LUCKY_KEYS = false,
        AP_EXEC_BUILD_KEYS = false,
        AP_EXEC_BLOCK_SELECTIVITY = false,
        AP_EXEC_MAYBE_MATCH = false,
        AP_EXEC_CHECK = false,
        AP_EXEC_JOIN_RESULT = false,
        AP_EXEC_EMIT = false,

        TIMING = true,

        CONTROL = true;

    static const char* debug_output = "./debug_output.txt";

    template<typename ...Arg>
    inline void DEBUG_LOG(bool config, const char* format, Arg... args) {
        if (CONTROL && config) std::printf(format, args...);
    }

    inline void DEBUG_LOG(bool config, const char* format) {
        if (CONTROL && config) std::printf(format);
    }

    template<typename ...Arg>
    inline void ERROR_LOG(const char* format, Arg... args) {
        std::printf("ERROR: ");
        std::printf(format, args...);
    }

    inline void ERROR_LOG(const char* format) {
        std::printf("ERROR: ");
        std::printf(format);
    }

    void debug_page(bool config, const page::page_id_t, buffer::BufferPoolManager*);
    void debug_leaf(const page::LeafPage* leaf);
    void debug_internal(const page::InternalPage* link);
    void debug_value(const page::ValuePage*);
    void debug_root(const page::RootPage*);

} // end namespace DB::debug