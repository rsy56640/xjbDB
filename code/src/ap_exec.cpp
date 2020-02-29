#include "ap_exec.h"
#include <cstring>

namespace DB::ap {

    block_tuple_iter_t::block_tuple_iter_t(const block_tuple_t* block_tuple)
        : block_tuple_(block_tuple), idx_(0) {}
    bool block_tuple_iter_t::is_end() const { return idx_ == VECTOR_SIZE; }
    bool block_tuple_iter_t::valid() const { return block_tuple_->select_[idx_]; }
    ap_row_t block_tuple_iter_t::getTuple() const { return block_tuple_->rows_[idx_]; }
    void block_tuple_iter_t::next() { idx_++; }


    VECTOR_INT block_tuple_t::getINT(page::range_t range) const {
        VECTOR_INT vec;
        // OPTIMIZATION: maybe we could use SIMD-gather
        // `_mm256_mmask_i32gather_epi32()` requires CPU flags "AVX512VL + AVX512F"
        // "AVX512" is supported on kightslanding, cascadelake and so on.
        for(int32_t i = 0; i < VECTOR_SIZE / 2; i++) {
            xjbDB_prefetch_on_array((char*)(rows_),
                                    i,
                                    sizeof(ap_row_t));
            xjbDB_prefetch_on_array((char*)(rows_),
                                    i + VECTOR_SIZE/2,
                                    sizeof(ap_row_t));
            vec[i] = rows_[i].getINT(range);
            vec[i + VECTOR_SIZE / 2] = rows_[i + VECTOR_SIZE / 2].getINT(range);
        }
        return vec;
    }


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
                block.select_[i] = true;
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


    ///////////////////////////////////////////////////////////////////////////
    ///////////////////////  hash table implementation  ///////////////////////
    ///////////////////////////////////////////////////////////////////////////


    hash_table_t::~hash_table_t() {
        delete[] bucket_size_;
        delete[] key_col_;
        delete[] bucket_head_;
        delete[] next_;
    }


    // return before `% BUCKET_AMOUNT`
    uint32_t hash2bucket(int32_t key) {
        return std::hash<uint32_t>()(key) % hash_table_t::BUCKET_AMOUNT;
    }
    // return before `% BUCKET_AMOUNT`
    VECTOR_INT hash2bucket(VECTOR_INT keys) {
        // OPTIMIZATION: SIMD mod
        for(uint32_t i = 0; i < VECTOR_SIZE; i++) {
            keys[i] = hash2bucket(keys[i]);
        }
        return keys;
    }


    void hash_table_t::insert(const block_tuple_t& block) {
        // OPTIMIZATION: use SIMD gather to get key-col, SIMD hash, then SIMD scatter-add?
        for(block_tuple_iter_t it = block.first(); !it.is_end(); it.next()) {
            if(likely(it.valid())) {
                const ap_row_t tuple = it.getTuple();
                const int32_t key = tuple.getINT(left_);
                lucky_key_ = key;
                bucket_size_[hash2bucket(key)]++;
                row_buf_.push_back(tuple);
            }
        }
    }


    void hash_table_t::build() {
        const uint32_t N = row_buf_.size();

        key_col_ = new int32_t[N + 1];
        bucket_head_ = new int32_t[BUCKET_AMOUNT];
        next_ = new int32_t[N + 1];
        key2rowid_.resize(N + 1);

        // compute `bucket_head_`
        uint32_t count = 1;
        for(uint32_t i = 0; i < BUCKET_AMOUNT; i++) {
            const uint32_t bucket_cnt_i = bucket_size_[i];
            if(bucket_cnt_i == 0) {
                bucket_head_[i] = 0;
            }
            else {
                bucket_head_[i] = count;
            }
            count += bucket_cnt_i;
        }

        // reuse `bucket_size_` as `bucket_head_` for build `key_col_`
        std::memcpy(bucket_size_, bucket_head_, BUCKET_AMOUNT * sizeof(uint32_t));
        int32_t* bucket_head = bucket_size_;

        // build `key_col_` (btw, `key2rowid_`)
        key_col_[0] = lucky_key_;
        auto it = row_buf_.cbegin();
        for(uint32_t i = 1; i <= N; i++, ++it) {
            const int32_t key = it->getINT(left_);
            const uint32_t bucket_no = hash2bucket(key);
            key_col_[bucket_head[bucket_no]] = key;
            key2rowid_[bucket_head[bucket_no]] = i - 1;
            bucket_head[bucket_no]++;
        }

        // prepare `next_`
        for(uint32_t i = 1; i <= N; i++) {
            next_[i] = true;
        }
        next_[N] = false;
        for(uint32_t i = 0; i < BUCKET_AMOUNT; i++) {
            const uint32_t head_i = bucket_head_[i];
            if(likely(head_i != 0)) {
                next_[head_i - 1] = false;
            }
        }
    }


    static
    ap_row_t splice(const ap_row_t& left, const ap_row_t& right, uint32_t left_len, uint32_t right_len) {


    }


    join_result_buf_t hash_table_t::probe(const block_tuple_t& block) const {
        join_result_buf_t result;
        const VECTOR_INT probe_keys = block.getINT(right_);
        // pos: probe[i] might be equal to build[pos[i]]
        VECTOR_INT pos = gatheri32(bucket_head_, hash2bucket(probe_keys));
        // maybe_match (0-1 mask vector): maybe_match[i] == true => probe[i] should be checked
        VECTOR_INT maybe_match = simd_int2bool(pos);

        for(;;) {
            VECTOR_INT build_keys =
                mask_gatheri32(build_keys, key_col_, pos, zero_one_mask_2_vector_mask(maybe_match));

            // Do not need to worry about NULL-key in empty bucket,
            // we use a lucky key to prevent corresponding keys being equally.
            VECTOR_INT check = simd_compare_eq(build_keys, probe_keys);

            // materialize join result
            // TODO:

            

            if(left_unique_) {
                maybe_match = maybe_match - check;
            }

            VECTOR_INT next_round = mask_gatheri32(ZERO_VEC, next_, pos, zero_one_mask_2_vector_mask(maybe_match));
            next_round = next_round & maybe_match;

            VECTOR_BOOL flag = (next_round == ZERO_VEC);
            if(simd_all_eq(flag, TRUE_VEC)) {
                break;
            }

            pos = pos + next_round;
        }

        return result;
    }


} // end namespace DB::ap