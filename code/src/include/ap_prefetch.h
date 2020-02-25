#include <emmintrin.h>

namespace DB::ap {
    constexpr int CACHELINE_SIZE = 128;
    inline
    void xjbDB_prefetch_on_array(
        char* addr,
        int block_index,
        int block_size)
    {
        _mm_prefetch(addr + 
                     (block_index + 1 + CACHELINE_SIZE/block_size) * block_size,
                     _MM_HINT_T0);
    }
}// end namespace DB::ap