#include "include/txn.h"
#include "include/scoped_guard.h"

namespace DB::txn
{

    inline ts_t consume_ts() { return ++global_ts_; }
    inline ts_t get_ts() { return global_ts_; }


    // fake util implementation for sake of branch "mvcc-trial"
    page::record_t* create_record() {
        return new page::record_t{};
    }
    page::delta_record_t* create_delta() {
        return new page::delta_record_t{};
    }
    void recycle_delta(page::delta_record_t* delta) {
        delete delta;
    }

    ///////////////////////////////////////////////////////////////////////////////////

    txn_t::txn_t(vm::VM* vm) :vm_(vm), begin_ts_(consume_ts()) {}

    void txn_t::commit() {

    }

    void txn_t::abort() {

    }


    // validation thread
    void validation_routine(txn_t* txn)
    {
        // only one guy can do validation at one time
        while (validation_flag.test_and_set());
        defer[]() { validation_flag.clear(); };

        // *** validate read set ***
        // we get commit_ts later, and validate [begin, commit] is same as [begin, now],
        // since only me is in validation phase, thus no one other can commit.
        const ts_t begin_ts = txn->begin_ts_;
        //const ts_t validation_ts = get_ts();
        bool ok = true;

        for (txn_t::read_info_t read_info : txn->read_set) {
            page::record_t* record = read_info.record_ptr_;
            ts_t last_commit_ts = record->commit_ts_;
            page::delta_record_t* next = record->next_delta_;
            while (next != nullptr) {
                last_commit_ts = next->commit_ts_;
                if (last_commit_ts > begin_ts) {
                    if (read_info.col_num_ == next->col_num_) {
                        ok = false;
                        break;
                    }
                }
                next = next->next_delta_;
            }
            if (!ok)
                break;
        }

        if (!ok) {
            txn->abort();
            return;
        }

        // *** commit ***
        // 1. hold commit_col_lock
        // 2. get commit_ts
        // 3. update new delta
        // 4. release commit_col_lock

        for (auto& write_info : txn->write_set)
            write_info.record_ptr_->set_commit_col(write_info.col_num_);

        const ts_t commit_ts = consume_ts();

        for (auto& write_info : txn->write_set) {
            page::record_t* record = write_info.record_ptr_;
            page::delta_record_t** next = &record->next_delta_;
            while (*next != nullptr)
                next = &((*next)->next_delta_);
            for (page::delta_record_t* new_delta : write_info.deltas_) {
                new_delta->commit_ts_ = commit_ts;
                *next = new_delta;
                next = &(*next)->next_delta_;
            }
        }

        for (auto& write_info : txn->write_set)
            write_info.record_ptr_->clear_commit_col();

        txn->commit();

    } // end `void validation_routine(txn_t* txn)`


} // end namespace DB::txn