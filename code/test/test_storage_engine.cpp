#ifdef _xjbDB_test_STORAGE_ENGINE_
#include "test.h"
#include <map>
#include <cstring>
#include <iostream>
#include <memory>
#include <random>
#include <vector>
#include <algorithm>
#include <ctime>
using namespace DB::tree;
using namespace std;

void test_create(vm::VM& vm_)
{
    vm_.test_create_table();

    KeyEntry key;
    key.key_t = page::key_t_t::INTEGER;
    page::ValueEntry value;
    value.value_state_ = value_state::INUSED;

    KVEntry kv = { key ,value };

    //
    // insert test
    //
    vector<int> keys = { 1, 3, 6, 98, 77, 5, 12, 44, 23 };
    for (int i : keys)
    {
        key.key_int = i;
        set(value, s[i % (sizeof(s) / sizeof(const char*))]);
        auto ret1 = vm_.test_insert({ key , value });
    }

}


void test_rebuild(vm::VM& vm_)
{
    vm_.test_output();
}


void test()
{

    printf("--------------------- test begin ---------------------\n");

    vm::VM vm_;
    vm_.init();

    int cmd;
    scanf("%d", &cmd);
    if (cmd == 0)
        test_create(vm_);
    else
        test_rebuild(vm_);

    vm_.test_flush();


    printf("--------------------- test end ---------------------\n");

}

#endif // _xjbDB_test_STORAGE_ENGINE_