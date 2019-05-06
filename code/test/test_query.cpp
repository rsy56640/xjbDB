#ifdef _xjbDB_TEST_QUERY_
#include "../src/include/vm.h"
#include "../src/include/ast.h"
#include "../src/include/query.h"
#include <cstdio>
#include <iostream>
#include <string>
using namespace std;
using namespace DB;
using namespace DB::query;


void test()
{

    printf("--------------------- test begin ---------------------\n");

    vm::VM vm_;
    vm_.init();

    std::string sql;
    cin >> sql;
    SQLValue plan = query::sql_parse(sql);

    printf("--------------------- test end ---------------------\n");

}

#endif // _xjbDB_TEST_QUERY_