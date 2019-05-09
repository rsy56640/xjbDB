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

	char buffer[256];
	cin.getline(buffer, 256);
    std::string sql(buffer);
    SQLValue plan = query::sql_parse(sql);

    printf("--------------------- test end ---------------------\n");

}

#endif // _xjbDB_TEST_QUERY_