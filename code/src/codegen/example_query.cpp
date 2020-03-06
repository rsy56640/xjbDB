//////////////////////////////////////////////////////////////////
//////////////////////  example of codegen  //////////////////////
//////////////////////////////////////////////////////////////////
#include "ap_exec.h"

static
DB::ap::block_tuple_t example_projection(const DB::ap:: block_tuple_t& block) {
    return block;
}

extern "C"
DB::ap::VMEmitOp example_query(const DB::ap::ap_table_array_t& tables) {
    const DB::ap::ap_table_t& T1 = tables.at(1);
    const DB::ap::ap_table_t& T2 = tables.at(2);
    const DB::ap::ap_table_t& T3 = tables.at(3);
    DB::page::range_t rng1{ 0, 4 };
    DB::page::range_t rng2{ 4, 4};
    DB::page::range_t rng3{ 4, 4 };
    DB::page::range_t rng_j2{ 0, 4 };
    DB::ap::hash_table_t ht1(rng1, rng_j2, 8, 8, true);
    DB::ap::hash_table_t ht2(rng2, rng3, 8, 16, false);
    DB::ap::VMEmitOp emit;


    for(DB::ap::ap_block_iter_t it = T1.get_block_iter(); !it.is_end();) {
        DB::ap::block_tuple_t block = it.consume_block();

        block.selectivity_and(block.getINT({ 4, 4 }) > 42);

        ht1.insert(block);
    }
    ht1.build();


    for(DB::ap::ap_block_iter_t it = T2.get_block_iter(); !it.is_end();) {
        DB::ap::block_tuple_t block = it.consume_block();

        block.selectivity_and(block.getINT({ 4, 4 }) < 233);

        ht2.insert(block);
    }
    ht2.build();


    for(DB::ap::ap_block_iter_t it = T3.get_block_iter(); !it.is_end();) {
        DB::ap::block_tuple_t block = it.consume_block();

        DB::ap::join_result_buf_t join_result2 = ht2.probe(block);
        for(DB::ap::ap_block_iter_t it = join_result2.get_block_iter(); !it.is_end();) {
            DB::ap::block_tuple_t block = it.consume_block();

            DB::ap::join_result_buf_t join_result1 = ht1.probe(block);
            for(DB::ap::ap_block_iter_t it = join_result2.get_block_iter(); !it.is_end();) {
                DB::ap::block_tuple_t block = it.consume_block();

                block = example_projection(block);

                emit.emit(block);
            }
        }
    }

    return emit;

} // end example_codegen function