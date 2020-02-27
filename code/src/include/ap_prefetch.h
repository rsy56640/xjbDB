#include <emmintrin.h>

namespace DB::ap {
    constexpr int CACHELINE_SIZE = 128;
    inline
    void xjbDB_prefetch_on_array(
        char* addr,
        int index,
        int scale)
    {
        _mm_prefetch(addr + 
                     (index + 1 + CACHELINE_SIZE/scale) * scale,
                     _MM_HINT_T0);
    }
}// end namespace DB::ap