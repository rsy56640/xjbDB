#pragma once
#include <deque>
#include <vector>
#include <string_view>
#include "ap_prefetch.h"
#include "page.h"

namespace DB::ap {

    constexpr uint32_t VECTOR_SIZE = 8;

    class ap_row_t {
    public:
        int32_t getINT(page::range_t range) const { return page::get_range_INT(row, range); }
        std::string_view getVARCHAR(page::range_t range) const { return std::string_view{ row.content_ + range.begin, range.len }; }
    private:
        page::ValueEntry row;
    };

    /*
     * iterate block row-wisely, mainly for complex condition
     */
    class block_tuple_t;
    class block_tuple_iter_t {
    public:
        block_tuple_iter_t(block_tuple_t* block_tuple)
            : block_tuple_(block_tuple), idx_(0) {}
        bool is_end() const { return idx_ == VECTOR_SIZE; };
        bool valid() const { return block_tuple_->select_[idx_]; };
        ap_row_t getTuple() const { return block_tuple_->rows_[idx_]; };
        void next() { idx_++; }
    private:
        block_tuple_t* block_tuple_;
        uint32_t idx_;
    };


    class block_tuple_t;
    struct VECTOR_INT {
        block_tuple_t* block_;
        int32_t vec_[VECTOR_SIZE];
        VECTOR_INT(block_tuple_t* block, page::range_t range);
        // SIMD compare, SIMD and
        void compare_equal(int32_t value);
        void compare_less_than(int32_t value);
        void compare_less_than_or_equal_to(int32_t value);
        void compare_greater_than(int32_t value);
        void compare_greater_than_or_equal_to(int32_t value);
    };
    /*
     * APNode input
     */
    class block_tuple_t {
        friend class block_tuple_iter_t;
        friend class VECTOR_INT;
    public:
        // for row-wise iteration
        block_tuple_iter_t first() { return block_tuple_iter_t{this}; }
        // for vector-wise SIMD execution
        VECTOR_INT getINT(page::range_t range) { return VECTOR_INT{ this, range }; }
    private:
        ap_row_t rows_[VECTOR_SIZE];
        bool select_[VECTOR_SIZE];
    };

    /*
     * ap_row_iter_t :
     *      holds all tuple content in an iterator-like object
     */
    class ap_table_t;
    class join_result_buf_t;
    class ap_block_iter_t {
    public:
        ap_block_iter_t(const ap_table_t* table);
        ap_block_iter_t(const join_result_buf_t* table);
        bool is_end() const;
        block_tuple_t consume_block();
    private:

    };

    class ap_table_t {
    public:
        uint32_t size() const { return rows_.size(); }
        ap_block_iter_t get_block_iter() const { return ap_block_iter_t{this}; }
    private:
        std::deque<ap_row_t> rows_;
    };

    class ap_table_array_t {
        std::deque<ap_table_t> tables_;
    public:
        const ap_table_t& at(uint32_t index) const { return tables_[index]; }
    };


    class VMEmitOp {
    public:
        void emit(const block_tuple_t&);
    private:

    };


    class join_result_buf_t {
    public:
        ap_block_iter_t get_block_iter() const { return ap_block_iter_t{this}; }
    private:
        std::deque<ap_row_t> rows_;
    };


    class hash_table_t {
    public:
        hash_table_t(page::range_t left, page::range_t right)
            :left_(left), right_(right) {}
        void insert(const block_tuple_t&);
        void build();
        join_result_buf_t probe(const block_tuple_t&);
    private:
        page::range_t left_, right_;
        std::vector<int32_t> key_col_;
        std::vector<ap_row_t> row_col_;
    };


//////////////////////////////////////////////////////////////////
//////////////////////  example of codegen  //////////////////////
//////////////////////////////////////////////////////////////////

block_tuple_t projection(const block_tuple_t&);

VMEmitOp query(const ap_table_array_t& tables) {
    const ap_table_t& T1 = tables.at(1);
    const ap_table_t& T2 = tables.at(2);
    const ap_table_t& T3 = tables.at(3);
    page::range_t rng1{ 0, 4 };
    page::range_t rng2{ 4, 4};
    page::range_t rng3{ 4, 4 };
    page::range_t rng_j2{ 0, 4 };
    hash_table_t ht1(rng1, rng_j2);
    hash_table_t ht2(rng2, rng3);
    VMEmitOp emit;


    for(ap_block_iter_t it = T1.get_block_iter(); !it.is_end();) {
        block_tuple_t block = it.consume_block();

        block.getINT({ 4, 4 }).compare_greater_than(42);

        ht1.insert(block);
    }
    ht1.build();


    for(ap_block_iter_t it = T2.get_block_iter(); !it.is_end();) {
        block_tuple_t block = it.consume_block();

        block.getINT({ 4, 4 }).compare_less_than(233);
        
        ht2.insert(block);
    }
    ht2.build();


    for(ap_block_iter_t it = T3.get_block_iter(); !it.is_end();) {
        block_tuple_t block = it.consume_block();

        join_result_buf_t join_result2 = ht2.probe(block);
        for(ap_block_iter_t it = join_result2.get_block_iter(); !it.is_end();) {
            block_tuple_t block = it.consume_block();

            join_result_buf_t join_result1 = ht1.probe(block);
            for(ap_block_iter_t it = join_result2.get_block_iter(); !it.is_end();) {
                block_tuple_t block = it.consume_block();

                block = projection(block);

                emit.emit(block);
            }
        }
    }

    return emit;

} // end codegen function




} // end namespace DB::ap