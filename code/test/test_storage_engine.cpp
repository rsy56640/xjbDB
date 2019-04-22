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
    constexpr int key_test_insert_size = 666;
    constexpr int key_test_erase_size = 333;
    constexpr int key_test_find_size = 128;
    constexpr int rand_seed = 19260817;
    srand(time(0) + rand_seed);

    std::vector<int> keys(key_test_insert_size);
    for (int i = 0; i < key_test_insert_size; i++)
        keys[i] = i;
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(keys.begin(), keys.end(), g);

    for (int i : keys)
    {
        key.key_int = i;
        set(value, s[i % (sizeof(s) / sizeof(const char*))]);
        vm_.test_insert({ key , value });
    }

    for (int i = 0; i < key_test_erase_size; i++)
    {
        cout << endl;
        key.key_int = ((rand() % key_test_insert_size) + key_test_insert_size) % key_test_insert_size;
        vm_.test_erase(key);
    }

    vm_.test_size();

}


void test_rebuild(vm::VM& vm_)
{
    vm_.test_output();
    vm_.test_size();
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