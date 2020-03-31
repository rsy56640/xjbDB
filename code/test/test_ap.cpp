#ifdef _xjbDB_TEST_AP_
#include "test.h"
#include <cstdio>
#include <random>
#include <string>
#include <deque>
#include <iterator>

static std::random_device rd;
static std::mt19937 gen(rd());

static constexpr int MIN_TID = 1;
static constexpr int MAX_TID = 4;
static constexpr int MIN_CID = 1;
static constexpr int MAX_CID = 10;
static constexpr int MIN_SID = 1;
static constexpr int MAX_SID = 10240;


static inline
int get_uniform_tid() {
    static std::uniform_int_distribution<> uniform_tid(MIN_TID, MAX_TID);
    return uniform_tid(gen);
}

static inline
int get_skew_tid() {
    static double tid[] = { 1, 2, 3, 4, 5 }; // 1-4
    static double weight[] = { 1, 2, 3, 4 };
    std::piecewise_constant_distribution<> d(std::begin(tid), std::end(tid),
                                             std::begin(weight));
    return static_cast<int>(d(gen));
}

static inline
int get_normal_score() {
    static std::normal_distribution<> normal_score{ 60 , 10 };
    int score = normal_score(gen);
    if(score < 0)
        score = 0;
    else if(score > 100)
        score = 100;
    return score;
}

static std::string get_random_name() {
    static std::binomial_distribution<> d(6, 0.4);
    static std::uniform_int_distribution<> uniform_char(0, 25);
    const int name_len = d(gen) + 2;
    std::string name;
    for(int i = 0; i < name_len; i++) {
        name += "abcdefjhijklmnopqrstuvwxyz"[uniform_char(gen)];
    }
    name[0] -= 'a' - 'A';
    return name;
}


static void create_schema(vm::VM& vm) {
    vm.add_sql("CREATE TABLE Teacher(tid INT PK, name VARCHAR(8))");
    vm.add_sql("CREATE TABLE Course(cid INT PK, tid INT REFERENCES Teacher, subject VARCHAR(12))");
    vm.add_sql("CREATE TABLE Student(sid INT PK, name VARCHAR(8))");
    vm.add_sql("CREATE TABLE Score(sid INT REFERENCES Student, cid INT REFERENCES Course, score INT)");
    vm.add_sql("CREATE TABLE Class(tid INT REFERENCES Teacher, sid INT REFERENCES Student)");

    vm.add_sql("INSERT Teacher(tid, name) VALUES(1, \"Alexis\")");
    vm.add_sql("INSERT Teacher(tid, name) VALUES(2, \"Bob\")");
    vm.add_sql("INSERT Teacher(tid, name) VALUES(3, \"Carter\")");
    vm.add_sql("INSERT Teacher(tid, name) VALUES(4, \"Davies\")");

    vm.add_sql("INSERT Course(cid, tid, subject) VALUES(1, 1, \"English\")");
    vm.add_sql("INSERT Course(cid, tid, subject) VALUES(2, 2, \"Chinese\")");
    vm.add_sql("INSERT Course(cid, tid, subject) VALUES(3, 2, \"Music\")");
    vm.add_sql("INSERT Course(cid, tid, subject) VALUES(4, 3, \"Math\")");
    vm.add_sql("INSERT Course(cid, tid, subject) VALUES(5, 3, \"Football\")");
    vm.add_sql("INSERT Course(cid, tid, subject) VALUES(6, 3, \"Literature\")");
    vm.add_sql("INSERT Course(cid, tid, subject) VALUES(7, 4, \"History\")");
    vm.add_sql("INSERT Course(cid, tid, subject) VALUES(8, 4, \"Computer\")");
    vm.add_sql("INSERT Course(cid, tid, subject) VALUES(9, 4, \"Economics\")");
    vm.add_sql("INSERT Course(cid, tid, subject) VALUES(10, 4, \"Physics\")");
}


static void create_scattered_data(vm::VM& vm) {
    // create student
    std::deque<int> students;
    const std::string insert_student = "INSERT Student(sid, name) VALUES("; //1, "aaa")";
    for(int i = MIN_SID; i <= MAX_SID; i++) {
        //if(d(gen)) {
            students.push_back(i);
            std::string insert_student_sql = insert_student + std::to_string(i) + ", \"";
            insert_student_sql += get_random_name() + "\")";
            vm.add_sql(insert_student_sql.c_str());
        //}
    }

    // create class
    const std::string insert_class = "INSERT Class(tid, sid) VALUES("; // 1, 777)";
    for(int sid: students) {
        std::string insert_class_sql = insert_class + std::to_string(get_uniform_tid()) + ", ";
        insert_class_sql += std::to_string(sid) + ")";
        vm.add_sql(insert_class_sql.c_str());
    }

    // create score
    const std::string insert_score = "INSERT Score(sid, cid, score) VALUES("; //666, 2, 100)";
    for(int sid: students) {
        for(int cid = MIN_CID; cid <= MAX_CID; cid++) {
            std::string insert_score_sql = insert_score
                                        + std::to_string(sid) + ", "
                                        + std::to_string(cid) + ", ";
            insert_score_sql += std::to_string(get_normal_score()) + ")";
            vm.add_sql(insert_score_sql.c_str());
        }
    }
}


static void create_skewed_data(vm::VM& vm) {
    // create student
    std::deque<int> students;
    const std::string insert_student = "INSERT Student(sid, name) VALUES("; //1, "aaa")";
    for(int i = MIN_SID; i <= MAX_SID; i++) {
        //if(d(gen)) {
            students.push_back(i);
            std::string insert_student_sql = insert_student + std::to_string(i) + ", \"";
            insert_student_sql += get_random_name() + "\")";
            vm.add_sql(insert_student_sql.c_str());
        //}
    }

    // create class
    const std::string insert_class = "INSERT Class(tid, sid) VALUES("; // 1, 777)";
    for(int sid: students) {
        std::string insert_class_sql = insert_class + std::to_string(get_skew_tid()) + ", ";
        insert_class_sql += std::to_string(sid) + ")";
        vm.add_sql(insert_class_sql.c_str());
    }

    // create score
    const std::string insert_score = "INSERT Score(sid, cid, score) VALUES("; //666, 2, 100)";
    for(int sid: students) {
        for(int cid = MIN_CID; cid <= MAX_CID; cid++) {
            std::string insert_score_sql = insert_score
                                        + std::to_string(sid) + ", "
                                        + std::to_string(cid) + ", ";
            insert_score_sql += std::to_string(get_normal_score()) + ")";
            vm.add_sql(insert_score_sql.c_str());
        }
    }
}


static void create_simple_big_table(vm::VM& vm) {
    vm.add_sql("CREATE TABLE A(id INT PK, value INT)");
    vm.add_sql("CREATE TABLE B(id INT PK, value INT)");
    vm.add_sql("CREATE TABLE C(id INT PK, value INT)");
    vm.add_sql("CREATE TABLE D(id INT PK, value INT)");

    auto insert = [&vm](char table, int id, int value) {
        static char insert_base[64] = "INSERT A(id, value) VALUES(";
        insert_base[7] = table;
        sprintf(insert_base + 27, "%d,%d)\0", id, value);
        vm.add_sql(std::string(insert_base));
    };

    for(int i = 0; i < 1024; i++)
        insert('A', i, get_normal_score());
    for(int i = 0; i < 4096; i++)
        insert('B', i, get_normal_score());
    for(int i = 0; i < 2333; i++)
        insert('C', i, get_normal_score());
    for(int i = 0; i < 8888; i++)
        insert('D', i, get_normal_score());
}


void test()
{
    printf("--------------------- test begin ---------------------\n");

    printf("enter: (x, y)\n");
    printf("\t x=0 for query test.\n");
    printf("\t x=1 for create scattered data.\n");
    printf("\t x=2 for create skewed data.\n");
    printf("\t x=3 for create simple big data.\n");
    printf("\t y=0 for continuing after create\n");
    printf("\t y=1 for existing after create.\n");
    int x, y;
    std::cin >> x >> y;
    printf("[x=%d] [y=%d]\n", x, y);

    vm::VM vm_;
    vm_.init();

    if(x == 1 || x == 2) {
        create_schema(vm_);
    }

    switch(x) {
        case 0:
            break;
        case 1:
            create_scattered_data(vm_);
            break;
        case 2:
            create_skewed_data(vm_);
            break;
        case 3:
            create_simple_big_table(vm_);
            break;
        default:
            std::cout << "invalid x=" << x << std::endl;
    }

    if(y == 1) {
        vm_.add_sql("SCHEMA");
        vm_.add_sql("EXIT");
    }

    vm_.start();

    printf("--------------------- test end ---------------------\n");
}

#endif // _xjbDB_TEST_AP_