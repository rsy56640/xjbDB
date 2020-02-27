#include "ap_exec.h"

namespace DB::ap {

    block_tuple_iter_t::block_tuple_iter_t(block_tuple_t* block_tuple)
        : block_tuple_(block_tuple), idx_(0) {}
    bool block_tuple_iter_t::is_end() const { return idx_ == VECTOR_SIZE; }
    bool block_tuple_iter_t::valid() const { return block_tuple_->select_.select_[idx_]; }
    ap_row_t block_tuple_iter_t::getTuple() const { return block_tuple_->rows_[idx_]; }
    void block_tuple_iter_t::next() { idx_++; }


    VECTOR_INT::VECTOR_INT(block_tuple_t* block, page::range_t range)
    {
        // OPTIMIZATION: maybe we could use SIMD-gather
        for(int32_t i = 0; i < VECTOR_SIZE / 2; i++) {
            xjbDB_prefetch_on_array((char*)(block->rows_),
                                    i,
                                    sizeof(ap_row_t));
            xjbDB_prefetch_on_array((char*)(block->rows_),
                                    i+VECTOR_SIZE/2,
                                    sizeof(ap_row_t));
            vec_[i] = block->rows_[i].getINT(range);
            vec_[i + VECTOR_SIZE / 2] = block->rows_[i + VECTOR_SIZE / 2].getINT(range);
        }
    }


    // SIMD arithmatic
    VECTOR_BOOL operator&(VECTOR_BOOL, VECTOR_BOOL);
    VECTOR_BOOL& operator&=(VECTOR_BOOL, VECTOR_BOOL);
    VECTOR_BOOL operator|(VECTOR_BOOL, VECTOR_BOOL);
    VECTOR_BOOL& operator|=(VECTOR_BOOL, VECTOR_BOOL);
    VECTOR_INT operator+(VECTOR_INT, int32_t);
    VECTOR_INT operator+(int32_t, VECTOR_INT);
    VECTOR_INT operator+(VECTOR_INT, VECTOR_INT);
    VECTOR_INT operator-(VECTOR_INT, int32_t);
    VECTOR_INT operator-(int32_t, VECTOR_INT);
    VECTOR_INT operator-(VECTOR_INT, VECTOR_INT);
    VECTOR_INT operator*(VECTOR_INT, int32_t);
    VECTOR_INT operator*(int32_t, VECTOR_INT);
    VECTOR_INT operator*(VECTOR_INT, VECTOR_INT);
    VECTOR_INT operator/(VECTOR_INT, int32_t);
    VECTOR_INT operator/(int32_t, VECTOR_INT);
    VECTOR_INT operator/(VECTOR_INT, VECTOR_INT);
    VECTOR_INT operator%(VECTOR_INT, int32_t);
    VECTOR_INT operator%(int32_t, VECTOR_INT);
    VECTOR_INT operator%(VECTOR_INT, VECTOR_INT);


    // SIMD compare
    VECTOR_BOOL operator==(VECTOR_INT, int32_t);
    VECTOR_BOOL operator==(int32_t, VECTOR_INT);
    VECTOR_BOOL operator==(VECTOR_INT, VECTOR_INT);
    VECTOR_BOOL operator<(VECTOR_INT, int32_t);
    VECTOR_BOOL operator<(int32_t, VECTOR_INT);
    VECTOR_BOOL operator<(VECTOR_INT, VECTOR_INT);
    VECTOR_BOOL operator<=(VECTOR_INT, int32_t);
    VECTOR_BOOL operator<=(int32_t, VECTOR_INT);
    VECTOR_BOOL operator<=(VECTOR_INT, VECTOR_INT);
    VECTOR_BOOL operator>(VECTOR_INT, int32_t);
    VECTOR_BOOL operator>(int32_t, VECTOR_INT);
    VECTOR_BOOL operator>(VECTOR_INT, VECTOR_INT);
    VECTOR_BOOL operator>=(VECTOR_INT, int32_t);
    VECTOR_BOOL operator>=(int32_t, VECTOR_INT);
    VECTOR_BOOL operator>=(VECTOR_INT, VECTOR_INT);



    ap_block_iter_t::ap_block_iter_t(const ap_table_t* table)
        :it_(table->rows_.cbegin()), end_(table->rows_.cend()) {}
    ap_block_iter_t::ap_block_iter_t(const join_result_buf_t* table)
        :it_(table->rows_.cbegin()), end_(table->rows_.cend()) {}
    bool ap_block_iter_t::is_end() const { return it_ == end_; }
    block_tuple_t ap_block_iter_t::consume_block() {
        block_tuple_t block;
        for(uint32_t i = 0; i < VECTOR_SIZE; i++, ++it_) {
            if(likely(it_ != end_)) {
                block.rows_[i] = *it_;
                block.select_.select_[i] = true;
            }
            else {
                break;
            }
        }
        return block;
    }


    void VMEmitOp::emit(const block_tuple_t& block) {
        for(uint32_t i = 0; i < VECTOR_SIZE; i++) {
            if(likely(block.select_[i])) {
                rows_.push_back(block.rows_[i]);
            }
        }
    }


    void hash_table_t::insert(const block_tuple_t&) {

    }
    void hash_table_t::build() {

    }
    join_result_buf_t hash_table_t::probe(const block_tuple_t&) {

    }


} // end namespace DB::ap