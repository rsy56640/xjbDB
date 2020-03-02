#pragma once
#include <deque>
#include <vector>
#include <string_view>
#include "ap_prefetch.h"
#include "ap_simd.h"
#include "page.h"

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

namespace DB::vm { class VM; }
namespace DB::ap {

    /*
     * ************************* data flow representation *************************
     * 
     * ap_table_t:
     *      array of tuples, iterated by block.
     * 
     * block_tuple_t:
     *      array of tuples with fixed size, is the input for each APNode.
     * 
     * join_result_buf_t:
     *      array of tuples, iterated by block, is the output of join probe.
     * 
     * ************************* *************************
     * 
     * ap_block_iter_t:
     *      get block from table or join-result
     * 
     * block_tuple_iter_t:
     *      for NON-SIMD use.
     * 
     * VECTOR_INT:
     *      for SIMD use.
     * 
     * VECTOR_BOOL:
     *      for SIMD use.
     * 
     */

    struct ap_row_t {
        int32_t getINT(page::range_t range) const { return page::get_range_INT(row, range); }
        std::string_view getVARCHAR(page::range_t range) const { return std::string_view{ row.content_ + range.begin, range.len }; }
        page::ValueEntry row;
    };

    /*
     * iterate block row-wisely, mainly for complex condition
     */
    class block_tuple_t;
    class block_tuple_iter_t {
    public:
        block_tuple_iter_t(const block_tuple_t* block_tuple);
        bool is_end() const;
        bool valid() const;
        ap_row_t getTuple() const;
        void next();
    private:
        const block_tuple_t* block_tuple_;
        uint32_t idx_;
    };


    /*
     * APNode input
     */
    class block_tuple_t {
        friend class block_tuple_iter_t;
        friend class ap_block_iter_t;
        friend class VECTOR_INT;
        friend class VMEmitOp;
        friend class hash_table_t;
    public:
        // for row-wise iteration
        block_tuple_iter_t first() const { return block_tuple_iter_t{this}; }
        // for vector-wise SIMD execution
        VECTOR_INT getINT(page::range_t range) const;
        void selectivity_and(VECTOR_INT mask) { select_ = select_ & mask; }
    private:
        ap_row_t rows_[VECTOR_SIZE];
        VECTOR_INT select_ = ZERO_VEC;
    };

    /*
     * ap_row_iter_t :
     *      holds all tuple content in an iterator-like object
     */
    class ap_table_t;
    class join_result_buf_t;
    class ap_block_iter_t {
        using ap_row_iter_t = std::deque<ap_row_t>::const_iterator;
    public:
        ap_block_iter_t(const ap_table_t* table);
        ap_block_iter_t(const join_result_buf_t* table);
        bool is_end() const;
        block_tuple_t consume_block();
    private:
        ap_row_iter_t it_;
        ap_row_iter_t end_;
    };

    class ap_table_t {
        friend class vm::VM;
        friend class ap_block_iter_t;
    public:
        uint32_t size() const { return rows_.size(); }
        ap_block_iter_t get_block_iter() const { return ap_block_iter_t{this}; }
    private:
        std::deque<ap_row_t> rows_;
    };

    class ap_table_array_t {
        friend class vm::VM;
    public:
        const ap_table_t& at(uint32_t index) const { return tables_[index]; }
    private:
        std::deque<ap_table_t> tables_;
    };


    class VMEmitOp {
    public:
        void emit(const block_tuple_t&);
    private:
        std::deque<ap_row_t> rows_;
    };


    class join_result_buf_t {
        friend class ap_block_iter_t;
        friend class hash_table_t;
    public:
        ap_block_iter_t get_block_iter() const { return ap_block_iter_t{this}; }
    private:
        std::deque<ap_row_t> rows_;
    };


    class hash_table_t {
    public:
        static constexpr uint32_t BUCKET_AMOUNT = 1 << 8;
    public:
        hash_table_t(page::range_t left, page::range_t right, uint32_t left_len, uint32_t right_len, bool left_unique)
            :left_(left), right_(right),
             left_len_(left_len), right_len_(right_len), left_unique_(left_unique),
             key2rowid_(), row_buf_()
             { bucket_size_ = new int32_t[BUCKET_AMOUNT]; }
        ~hash_table_t();
        hash_table_t(const hash_table_t&) = delete;
        hash_table_t& operator=(const hash_table_t&) = delete;
        hash_table_t(hash_table_t&&) = delete;
        hash_table_t& operator=(hash_table_t&&) = delete;

        void insert(const block_tuple_t&);
        void build();
        join_result_buf_t probe(const block_tuple_t&) const;

    private:
        const page::range_t left_, right_;
        const uint32_t left_len_, right_len_;
        const bool left_unique_;
        int32_t* bucket_size_;

        // since SIMD compare has no mask,
        // key_col_[0] must be an existing key.
        int32_t lucky_key_;

        // permute and rank
        int32_t* key_col_; // size = N + 1

        // record bucket head in `key_col_`
        int32_t* bucket_head_; // size = BUCKET_AMOUNT

        // record bucket chain
        int32_t* next_; // size = N + 1

        // record which row is mapped to the ey
        std::vector<uint32_t> key2rowid_; // size = N + 1

        // materialized row buffer
        std::deque<ap_row_t> row_buf_; // size = N
    };


    extern ap::VMEmitOp query(const ap::ap_table_array_t& tables);


} // end namespace DB::ap