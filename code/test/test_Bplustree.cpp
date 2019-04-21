#ifdef _xjbDB_test_BPLUSTREE_
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


void test()
{

    printf("--------------------- test begin ---------------------\n");

    OpenTableInfo info;
    info.isInit = true;
    std::shared_ptr<disk::DiskManager> disk_manager = std::make_shared<disk::DiskManager>();
    std::shared_ptr<vm::StorageEngine> storage_engine = std::make_shared<vm::StorageEngine>();
    std::shared_ptr<buffer::BufferPoolManager> buffer_pool_manager = std::make_shared<buffer::BufferPoolManager>(disk_manager.get());
    storage_engine->disk_manager_ = disk_manager.get();
    storage_engine->buffer_pool_manager_ = buffer_pool_manager.get();
    BTree bt(info, storage_engine->buffer_pool_manager_, page::key_t_t::INTEGER);

    map<int32_t, std::string> mp_bt;

    KeyEntry key;
    key.key_t = page::key_t_t::INTEGER;
    ValueEntry value;
    value.value_state_ = value_state::INUSED;

    KVEntry kv = { key ,value };

    constexpr int key_test_insert_size = 127;
    constexpr int key_test_erase_size = 666;
    constexpr int key_test_find_size = 128;
    constexpr int rand_seed = 19260817;
    srand(time(0) + rand_seed);

    std::vector<int> keys(key_test_insert_size);
    for (int i = 0; i < key_test_insert_size; i++)
        keys[i] = i;
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(keys.begin(), keys.end(), g);

    int find_error = 0, insert_error = 0, erase_error = 0;

    //
    // insert test
    //
    for (int i : keys)
    {
        key.key_int = i;
        set(value, s[i % (sizeof(s) / sizeof(const char*))]);
        auto ret1 = bt.insert({ key , value });
        auto ret2 = mp_bt.insert(std::make_pair(key.key_int, std::string(value.content_)));
        bool suc1 = ret1 == tree::INSERT_KV;
        bool suc2 = ret2.second;
        if (suc1 != suc2) {
            printf("insert error at [key = %d]\n", i);
            insert_error++;
        }
    }


    //
    // erase test
    //
    for (int i = 0; i < key_test_erase_size; i++)
    {
        cout << endl;
        key.key_int = ((rand() % key_test_insert_size) + key_test_insert_size) % key_test_insert_size;
        int ret1 = bt.erase(key);
        auto ret2 = mp_bt.erase(key.key_int);
        bool erase1 = ret1 == tree::ERASE_KV;
        bool erase2 = ret2 == 1;
        if (erase1 != erase2) {
            if (erase1)
                printf("erase error at [key = %d]: B+Tree should not has the key!!!\n", key.key_int);
            else
                printf("erase error at [key = %d]: B+Tree should has the key!!!\n", key.key_int);
            erase_error++;
            break;
        }
        else {
            if (erase1)
                printf("erase ok at [key = %d]\n", key.key_int);
            else
                printf("erase ok at [key = %d]: nothing\n", key.key_int);
        }
    }


    //
    // find test
    //
    for (int i = 0; i < key_test_find_size; i++)
    {
        cout << endl;
        key.key_int = ((rand() % key_test_insert_size) + key_test_insert_size) % key_test_insert_size;
        value = bt.find(key);
        cout << key.key_int << ": " << valueState[static_cast<int>(value.value_state_)]
            << " " << value.content_ << endl;
        auto ret2 = mp_bt.find(key.key_int);
        bool find1 = value.value_state_ == page::value_state::INUSED;
        bool find2 = ret2 != mp_bt.end();
        if (find1 != find2) {
            if (!find1)
                printf("find error at [key = %d]: key not found\n", key.key_int);
            else
                printf("find error at [key = %d]: key should not be found\n", key.key_int);
            find_error++;
        }
        else if (find1 && compare_v(value, ret2->second) != 0) {
            printf("find error at [key = %d]: value dismatched!!!\n", key.key_int);
            find_error++;
        }
    }


    bt.debug();

    printf("B+Tree size = %d\n", bt.size());
    printf("map size = %d\n", mp_bt.size());

    printf("find error = %d\n", find_error);
    printf("insert error = %d\n", insert_error);
    printf("erase error = %d\n", erase_error);
    printf("--------------------- test end ---------------------\n");

}

#endif // _xjbDB_test_BPLUSTREE_