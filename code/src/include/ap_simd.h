#pragma once
#include <immintrin.h>
#include <cstdint>

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

namespace DB::ap {

    constexpr uint32_t VECTOR_SIZE = 8;

    struct VECTOR_INT {
        __attribute__ ((aligned (32))) __m256i vec_;
        int& operator[](uint32_t index) { return (reinterpret_cast<int32_t*>(&vec_))[index]; }
        int operator[](uint32_t index) const { return (const_cast<int32_t*>(reinterpret_cast<const int32_t*>(&vec_)))[index]; }
    };
    inline VECTOR_INT get_vec(int32_t value) {
        return { _mm256_set_epi32(value, value, value, value, value, value, value, value) };
    }
    static VECTOR_INT ZERO_VEC = { get_vec(0x0) };
    static VECTOR_INT ONE_VEC = { get_vec(0x1) };
    static VECTOR_INT MAX_VEC = { get_vec(0x7FFFFFFF) };
    static VECTOR_INT MIN_VEC = { get_vec(0xFFFFFFFF) };


    struct VECTOR_BOOL {
        __attribute__ ((aligned (8))) __mmask8 mask_;
        bool& operator[](uint32_t index) { return (reinterpret_cast<bool*>(&mask_))[index]; }
        bool operator[](uint32_t index) const { return (const_cast<bool*>(reinterpret_cast<const bool*>(&mask_)))[index]; }
        VECTOR_BOOL& operator&=(VECTOR_BOOL mask) { mask_ &= mask.mask_; return *this; }
        VECTOR_BOOL& operator|=(VECTOR_BOOL mask) { mask_ |= mask.mask_; return *this; }
    };
    static VECTOR_BOOL TRUE_VEC = { 0xFF };
    static VECTOR_BOOL FALSE_VEC = { 0x0 };


    // SIMD arithmetic
    inline VECTOR_INT srl(VECTOR_INT vec) { return { _mm256_srli_epi32(vec.vec_, 1) }; }
    inline VECTOR_INT simd_mod256(VECTOR_INT vec) { return { _mm256_srli_epi32(_mm256_slli_epi32(vec.vec_, 24), 24) }; }

    inline VECTOR_INT operator+(VECTOR_INT vec1, VECTOR_INT vec2) { return { _mm256_add_epi32(vec1.vec_, vec2.vec_) }; }
    inline VECTOR_INT operator+(VECTOR_INT vec, int32_t value) { return vec + get_vec(value); }
    inline VECTOR_INT operator+(int32_t value, VECTOR_INT vec) { return get_vec(value) + vec; }

    inline VECTOR_INT operator-(VECTOR_INT vec1, VECTOR_INT vec2) { return { _mm256_sub_epi32(vec1.vec_, vec2.vec_) }; }
    inline VECTOR_INT operator-(VECTOR_INT vec, int32_t value) { return vec - get_vec(value); }
    inline VECTOR_INT operator-(int32_t value, VECTOR_INT vec) { return get_vec(value) - vec; }

    inline VECTOR_INT operator*(VECTOR_INT vec1, VECTOR_INT vec2) { return { _mm256_mul_epi32(vec1.vec_, vec2.vec_) }; }
    inline VECTOR_INT operator*(VECTOR_INT vec, int32_t value) { return vec * get_vec(value); }
    inline VECTOR_INT operator*(int32_t value, VECTOR_INT vec) { return get_vec(value) * vec; }


    // SIMD logical operation
    inline VECTOR_INT operator&(VECTOR_INT vec1, VECTOR_INT vec2) { return { _mm256_and_si256(vec1.vec_, vec2.vec_) }; }
    inline VECTOR_INT operator&(VECTOR_INT vec, int32_t value) { return vec & get_vec(value); }
    inline VECTOR_INT operator&(int32_t value, VECTOR_INT vec) { return get_vec(value) & vec; }

    inline VECTOR_INT operator|(VECTOR_INT vec1, VECTOR_INT vec2) { return { _mm256_or_si256(vec1.vec_, vec2.vec_) }; }
    inline VECTOR_INT operator|(VECTOR_INT vec, int32_t value) { return vec | get_vec(value); }
    inline VECTOR_INT operator|(int32_t value, VECTOR_INT vec) { return get_vec(value) | vec; }

    inline VECTOR_INT operator!(VECTOR_INT vec) { return ONE_VEC - vec; }
    inline VECTOR_INT operator~(VECTOR_INT vec) { return { _mm256_andnot_si256(vec.vec_, MIN_VEC.vec_) }; }
    inline VECTOR_INT simd_not1and2(VECTOR_INT vec1, VECTOR_INT vec2) { return { _mm256_andnot_si256(vec1.vec_, vec2.vec_) }; }

    // return 0-1 mask
    inline VECTOR_INT simd_compare_eq(VECTOR_INT vec1, VECTOR_INT vec2) { return { _mm256_srli_epi32(_mm256_cmpeq_epi32(vec1.vec_, vec2.vec_), 31) }; }
    inline VECTOR_INT simd_compare_eq(VECTOR_INT vec, int32_t value) { return simd_compare_eq(vec, get_vec(value)); }
    inline VECTOR_INT simd_compare_eq(int32_t value, VECTOR_INT vec) { return simd_compare_eq(get_vec(value), vec); }
    // simd == with 0-1 mask, return 0-1 mask
    inline VECTOR_INT simd_compare_eq_mask(VECTOR_INT vec1, VECTOR_INT vec2, VECTOR_INT mask) { return mask & VECTOR_INT{ _mm256_srli_epi32(_mm256_cmpeq_epi32(vec1.vec_, vec2.vec_), 31) }; }

    inline VECTOR_INT simd_int2bool(VECTOR_INT vec) {
        VECTOR_INT VEC_OVERFLOW = simd_compare_eq(vec, MIN_VEC);
        for(int32_t i = 0; i < 32; i++) { // in fact, 31 round is enough
            vec = srl(vec + 1);
        }
        return vec + VEC_OVERFLOW;
    }


    // SIMD compare

// Unfortunately, we donot have AVX512.
// In addition, the `_mmask8` support is bizard, which leads to us using `_mm256i` as mask.
/*
    inline VECTOR_BOOL operator==(VECTOR_INT vec1, VECTOR_INT vec2) { return { _mm256_cmpeq_epi32_mask(vec1.vec_, vec2.vec_) }; }
    inline VECTOR_BOOL operator==(VECTOR_INT vec, int32_t value) { return vec == get_vec(value); }
    inline VECTOR_BOOL operator==(int32_t value, VECTOR_INT vec) { return get_vec(value) == vec; }

    inline VECTOR_BOOL operator!=(VECTOR_INT vec1, VECTOR_INT vec2) { return { _mm256_cmpneq_epi32_mask(vec1.vec_, vec2.vec_) }; }
    inline VECTOR_BOOL operator!=(VECTOR_INT vec, int32_t value) { return vec != get_vec(value); }
    inline VECTOR_BOOL operator!=(int32_t value, VECTOR_INT vec) { return get_vec(value) != vec; }

    inline VECTOR_BOOL operator<(VECTOR_INT vec1, VECTOR_INT vec2) { return { _mm256_cmplt_epi32_mask(vec1.vec_, vec2.vec_) }; }
    inline VECTOR_BOOL operator<(VECTOR_INT vec, int32_t value) { return vec < get_vec(value); }
    inline VECTOR_BOOL operator<(int32_t value, VECTOR_INT vec) { return get_vec(value) < vec; }

    inline VECTOR_BOOL operator<=(VECTOR_INT vec1, VECTOR_INT vec2) { return { _mm256_cmple_epi32_mask(vec1.vec_, vec2.vec_) }; }
    inline VECTOR_BOOL operator<=(VECTOR_INT vec, int32_t value) { return vec <= get_vec(value); }
    inline VECTOR_BOOL operator<=(int32_t value, VECTOR_INT vec) { return get_vec(value) <= vec; }

    inline VECTOR_BOOL operator>(VECTOR_INT vec1, VECTOR_INT vec2) { return { _mm256_cmpgt_epi32_mask(vec1.vec_, vec2.vec_) }; }
    inline VECTOR_BOOL operator>(VECTOR_INT vec, int32_t value) { return vec > get_vec(value); }
    inline VECTOR_BOOL operator>(int32_t value, VECTOR_INT vec) { return get_vec(value) > vec; }

    inline VECTOR_BOOL operator>=(VECTOR_INT vec1, VECTOR_INT vec2) { return { _mm256_cmpge_epi32_mask(vec1.vec_, vec2.vec_) }; }
    inline VECTOR_BOOL operator>=(VECTOR_INT vec, int32_t value) { return vec >= get_vec(value); }
    inline VECTOR_BOOL operator>=(int32_t value, VECTOR_INT vec) { return get_vec(value) >= vec; }
*/

    inline VECTOR_INT operator==(VECTOR_INT vec1, VECTOR_INT vec2) { return { _mm256_cmpeq_epi32(vec1.vec_, vec2.vec_) }; }
    inline VECTOR_INT operator==(VECTOR_INT vec, int32_t value) { return vec == get_vec(value); }
    inline VECTOR_INT operator==(int32_t value, VECTOR_INT vec) { return get_vec(value) == vec; }

    inline VECTOR_INT operator!=(VECTOR_INT vec1, VECTOR_INT vec2) { return 1 - (vec1 == vec2); }
    inline VECTOR_INT operator!=(VECTOR_INT vec, int32_t value) { return vec != get_vec(value); }
    inline VECTOR_INT operator!=(int32_t value, VECTOR_INT vec) { return get_vec(value) != vec; }

    inline VECTOR_INT operator<(VECTOR_INT vec1, VECTOR_INT vec2) { return { _mm256_srli_epi32((vec1 - vec2).vec_, 31) }; }
    inline VECTOR_INT operator<(VECTOR_INT vec, int32_t value) { return vec < get_vec(value); }
    inline VECTOR_INT operator<(int32_t value, VECTOR_INT vec) { return get_vec(value) < vec; }

    inline VECTOR_INT operator<=(VECTOR_INT vec1, VECTOR_INT vec2) { return 1 - VECTOR_INT{ _mm256_srli_epi32((vec2 - vec1).vec_, 31) }; }
    inline VECTOR_INT operator<=(VECTOR_INT vec, int32_t value) { return vec <= get_vec(value); }
    inline VECTOR_INT operator<=(int32_t value, VECTOR_INT vec) { return get_vec(value) <= vec; }

    inline VECTOR_INT operator>(VECTOR_INT vec1, VECTOR_INT vec2) { return { _mm256_srli_epi32((vec2 - vec1).vec_, 31) }; }
    inline VECTOR_INT operator>(VECTOR_INT vec, int32_t value) { return vec > get_vec(value); }
    inline VECTOR_INT operator>(int32_t value, VECTOR_INT vec) { return get_vec(value) > vec; }

    inline VECTOR_INT operator>=(VECTOR_INT vec1, VECTOR_INT vec2) { return 1 - VECTOR_INT{ _mm256_srli_epi32((vec1 - vec2).vec_, 31) }; }
    inline VECTOR_INT operator>=(VECTOR_INT vec, int32_t value) { return vec >= get_vec(value); }
    inline VECTOR_INT operator>=(int32_t value, VECTOR_INT vec) { return get_vec(value) >= vec; }



    inline bool simd_all_eq(VECTOR_BOOL mask1, VECTOR_BOOL mask2) { return mask1.mask_ == mask2.mask_; }
    inline bool simd_all_eq(VECTOR_INT vec1, VECTOR_INT vec2) {
        // return simd_all_eq({ _mm256_cmpeq_epi32_mask(vec1.vec_, vec2.vec_) }, TRUE_VEC);
        // no SIMD support on AVX2
        for(uint32_t i = 0; i < VECTOR_SIZE; i++)
            if(likely(vec1[i] != vec2[i]))
                return false;
        return true;
    }
    inline bool simd_all_eq(VECTOR_INT vec, int value) { return simd_all_eq(vec, get_vec(value)); }

    // SIMD gather/scatter
    inline VECTOR_INT zero_one_mask_2_vector_mask(VECTOR_INT mask) { return { _mm256_slli_epi32(mask.vec_, 31) }; }
    inline VECTOR_INT simd_gatheri32(const int32_t* base, VECTOR_INT index) {
        return { _mm256_i32gather_epi32(base, index.vec_, sizeof(int32_t)) };
    }
    inline VECTOR_INT simd_mask_gatheri32(VECTOR_INT src, const int32_t* base, VECTOR_INT index, VECTOR_INT mask) {
        return { _mm256_mask_i32gather_epi32(src.vec_, base, index.vec_, mask.vec_, sizeof(int32_t)) };
    }

#ifdef AVX512
    inline void scatter(int32_t* base, VECTOR_INT index, VECTOR_INT vec, int32_t scale) {
        _mm256_i32scatter_epi32(base, index.vec_, vec.vec_, scale);
    }
#endif



} // end namespace DB::ap