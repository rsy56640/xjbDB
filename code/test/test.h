#ifndef _TEST_H
#define _TEST_H
#include "../src/include/disk_manager.h"
#include "../src/include/hash_lru.h"
#include "../src/include/buffer_pool.h"
#include "../src/include/vm.h"
#include "../src/include/BplusTree.h"
#include "../src/include/page.h"
using namespace DB;

static const char* s[] =
{
    "str0: hello world",
    "str1: qwer",
    "str2: The dog barks at the cat.",
    "str3: WTF!!!!",
    "str4: gkdgkd",
    "str5: LeetCode Completed",
    "str6: Distributed Database",
    "str7: abcdefghijk",
    "str8: hhhhhh",
    "str9: xjbDB ok",
};

static const char* valueState[2] = { "OBSOLETE", "INUSED" };

inline void set(page::ValueEntry& vEntry, const char* s) {
    const int size = strlen(s);
    std::memset(vEntry.content_, 0, sizeof(char)* page::MAX_TUPLE_SIZE);
    std::memcpy(vEntry.content_, s, size > page::MAX_TUPLE_SIZE ? page::MAX_TUPLE_SIZE : size);
}

inline int compare_v(const page::ValueEntry& vEntry, const std::string& s) {
    return std::string(vEntry.content_) < s;
}

void test();

#endif // !_TEST_H
