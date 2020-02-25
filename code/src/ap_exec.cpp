#include "ap_exec.h"

namespace DB::ap {

    VECTOR_INT::VECTOR_INT(block_tuple_t* block, page::range_t range)
        : block_(block)
    {
        for(int32_t i = 0; i < VECTOR_SIZE / 2; i++) {
            xjbDB_prefetch_on_array((char*)(block_->rows_),
                                    i,
                                    sizeof(ap_row_t));
            xjbDB_prefetch_on_array((char*)(block_->rows_),
                                    i+VECTOR_SIZE/2,
                                    sizeof(ap_row_t));
            vec_[i] = block_->rows_[i].getINT(range);
            vec_[i + VECTOR_SIZE / 2] = block_->rows_[i + VECTOR_SIZE / 2].getINT(range);
        }
    }
    void VECTOR_INT::compare_equal(int32_t value) {

    }
    void VECTOR_INT::compare_less_than(int32_t value) {

    }
    void VECTOR_INT::compare_less_than_or_equal_to(int32_t value) {

    }
    void VECTOR_INT::compare_greater_than(int32_t value) {

    }
    void VECTOR_INT::compare_greater_than_or_equal_to(int32_t value) {

    }


    ap_block_iter_t::ap_block_iter_t(const ap_table_t* table) {

    }
    ap_block_iter_t::ap_block_iter_t(const join_result_buf_t* table) {

    }
    bool ap_block_iter_t::is_end() const {

    }
    block_tuple_t ap_block_iter_t::consume_block() {

    }


    void VMEmitOp::emit(const block_tuple_t&) {

    }



    void hash_table_t::insert(const block_tuple_t&) {

    }
    void hash_table_t::build() {

    }
    join_result_buf_t hash_table_t::probe(const block_tuple_t&) {

    }


} // end namespace DB::ap