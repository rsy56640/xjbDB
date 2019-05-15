#ifndef _DEBUG_LOG_H
#define _DEBUG_LOG_H
#include <string>
#include <cstdio>
#include "page.h"

namespace DB::debug
{

	constexpr bool
		PAGE_REF = false,
		LRU_EVICT = false,
		BT_CREATE = false,

		BUFFER_FETCH = false,
		BUFFER_FLUSH = true,
		BUFFER_NEW = false,
		BUFFER_DELETE = false,

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

		QUERY_PROCESS = true,

		WAL = true,

		FLUSH = true,

		DESTROY_LOG = true,

		LEXER_LOG = false,
		PARSE_LOG = false,
		QUERY_LOG = false,

		PARSE_TREE_PNG = false,

        NON_DEBUG = false,
        xbjDB_DEBUG = true,

        CONTROL = true;


    // no use
    static const char* debug_output = "./debug_output.txt";


    template<typename ...Arg>
    inline void DEBUG_LOG(bool config, const char* format, Arg... args) {
        if (CONTROL && config) std::printf(format, args...);
    }

    template<typename ...Arg>
    inline void ERROR_LOG(const char* format, Arg... args) {
        std::printf("ERROR: ");
        std::printf(format, args...);
    }

    void debug_page(bool config, const page::page_id_t, buffer::BufferPoolManager*);
    void debug_leaf(const page::LeafPage* leaf);
    void debug_internal(const page::InternalPage* link);
    void debug_value(const page::ValuePage*);
    void debug_root(const page::RootPage*);

} // end namespace DB::debug

#endif // !_DEBUG_LOG_H