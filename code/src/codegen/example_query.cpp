//////////////////////////////////////////////////////////////////
//////////////////////  example of codegen  //////////////////////
//////////////////////////////////////////////////////////////////
#include "ap_exec.h"

namespace DB::ap {

    static
    block_tuple_t example_projection(const block_tuple_t& block) {
        return block;
    }

    extern
    VMEmitOp example_query(const ap_table_array_t& tables) {
        const ap_table_t& T1 = tables.at(1);
        const ap_table_t& T2 = tables.at(2);
        const ap_table_t& T3 = tables.at(3);
        page::range_t rng1{ 0, 4 };
        page::range_t rng2{ 4, 4};
        page::range_t rng3{ 4, 4 };
        page::range_t rng_j2{ 0, 4 };
        hash_table_t ht1(rng1, rng_j2, 8, 8, true);
        hash_table_t ht2(rng2, rng3, 8, 16, false);
        VMEmitOp emit;


        for(ap_block_iter_t it = T1.get_block_iter(); !it.is_end();) {
            block_tuple_t block = it.consume_block();

            block.selectivity_and(block.getINT({ 4, 4 }) > 42);

            ht1.insert(block);
        }
        ht1.build();


        for(ap_block_iter_t it = T2.get_block_iter(); !it.is_end();) {
            block_tuple_t block = it.consume_block();

            block.selectivity_and(block.getINT({ 4, 4 }) < 233);

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

                    block = example_projection(block);

                    emit.emit(block);
                }
            }
        }

        return emit;

    } // end example_codegen function


}