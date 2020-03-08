#ifdef _xjbDB_TEST_AP_
#include "test.h"
#include <random>
#include <string>
#include <deque>

static std::random_device rd;
static std::mt19937 gen(rd());

static constexpr int MIN_TID = 1;
static constexpr int MAX_TID = 4;
static constexpr int MIN_CID = 1;
static constexpr int MAX_CID = 10;
static constexpr int MIN_SID = 1;
static constexpr int MAX_SID = 1024;


static inline
int get_uniform_tid() {
    static std::uniform_int_distribution<> uniform_tid(MIN_TID, MAX_TID);
    return uniform_tid(gen);
}

static inline
int get_uniform_cid() {
    static std::uniform_int_distribution<> uniform_cid(MIN_CID, MAX_CID);
    return uniform_cid(gen);
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

static void create_scattered_data(vm::VM& vm) {
    
    static std::bernoulli_distribution d(0.75); // 3/4 true

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

}


void test()
{
    printf("--------------------- test begin ---------------------\n");

    printf("enter: (x, y)\n");
    printf("\t x=1 for create scattered data.\n");
    printf("\t x=2 for create skewed data.\n");
    printf("\t x=other for query test.\n");
    printf("\t y=0 for continuing after create\n");
    printf("\t y=other for existing after create.\n");
    int x, y;
    std::cin >> x >> y;
    printf("[x=%d] [y=%d]\n", x, y);

    vm::VM vm_;
    vm_.init();

    if(x == 1 || x == 2) {
        //vm_.add_sql("");
        vm_.add_sql("CREATE TABLE Teacher(tid INT PK, name VARCHAR(8))");
        vm_.add_sql("CREATE TABLE Course(cid INT PK, tid INT REFERENCES Teacher, subject VARCHAR(12))");
        vm_.add_sql("CREATE TABLE Student(sid INT PK, name VARCHAR(8))");
        vm_.add_sql("CREATE TABLE Score(sid INT REFERENCES Student, cid INT REFERENCES Course, score INT)");
        vm_.add_sql("CREATE TABLE Class(tid INT REFERENCES Teacher, sid INT REFERENCES Student)");

        vm_.add_sql("INSERT Teacher(tid, name) VALUES(1, \"Alexis\")");
        vm_.add_sql("INSERT Teacher(tid, name) VALUES(2, \"Bob\")");
        vm_.add_sql("INSERT Teacher(tid, name) VALUES(3, \"Carter\")");
        vm_.add_sql("INSERT Teacher(tid, name) VALUES(4, \"Davies\")");

        vm_.add_sql("INSERT Course(cid, tid, subject) VALUES(1, 1, \"English\")");
        vm_.add_sql("INSERT Course(cid, tid, subject) VALUES(2, 2, \"Chinese\")");
        vm_.add_sql("INSERT Course(cid, tid, subject) VALUES(3, 2, \"Music\")");
        vm_.add_sql("INSERT Course(cid, tid, subject) VALUES(4, 3, \"Math\")");
        vm_.add_sql("INSERT Course(cid, tid, subject) VALUES(5, 3, \"Football\")");
        vm_.add_sql("INSERT Course(cid, tid, subject) VALUES(6, 3, \"Literature\")");
        vm_.add_sql("INSERT Course(cid, tid, subject) VALUES(7, 4, \"History\")");
        vm_.add_sql("INSERT Course(cid, tid, subject) VALUES(8, 4, \"Computer\")");
        vm_.add_sql("INSERT Course(cid, tid, subject) VALUES(9, 4, \"Economics\")");
        vm_.add_sql("INSERT Course(cid, tid, subject) VALUES(10, 4, \"Physics\")");

        if(x == 1) {
            create_scattered_data(vm_);
        }
        else {
            create_skewed_data(vm_);
        }
        if(y != 0) {
            vm_.add_sql("SHOW");
            vm_.add_sql("EXIT");
        }
    }

    vm_.start();

    printf("--------------------- test end ---------------------\n");
}

#endif // _xjbDB_TEST_AP_