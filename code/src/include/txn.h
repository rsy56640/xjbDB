#ifndef _TXN_H
#define _TXN_H
#include "env.h"
#include "page.h"
#include "vm.h"
#include <vector>
#include <deque>

namespace DB::txn
{

    page::record_t* create_record();
    page::delta_record_t* create_delta();
    void recycle_delta(page::delta_record_t* delta);

    using ts_t = page::ts_t;
    std::atomic<ts_t> global_ts_ = 0;
    static std::atomic_flag validation_flag = ATOMIC_FLAG_INIT;

    struct txn_t
    {
        txn_t(vm::VM* vm);
        void commit();  // info vm
        void abort();   // info vm

        vm::VM* vm_;
        const ts_t begin_ts_;
        std::vector<query::SQLValue> ops_;

        struct read_info_t {
            page::record_t* record_ptr_;
            uint32_t col_num_;
        };
        struct write_info_t {
            page::record_t* record_ptr_;
            uint32_t col_num_;
            std::deque<page::delta_record_t*> deltas_;
        };
        std::deque<read_info_t> read_set;
        std::deque<write_info_t> write_set;
    };


    void validation_routine(txn_t* txn);


} // end namespace DB::txn

#endif // !_TXN_H