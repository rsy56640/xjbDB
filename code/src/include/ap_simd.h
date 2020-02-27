#pragma once
#include <immintrin.h>
#include "env.h"
#include "ap_exec.h"

namespace DB::ap {

    constexpr uint32_t VECTOR_SIZE = 8;

    struct VECTOR_INT {
        __attribute__ ((aligned (32))) __m256i vec_;
        int& operator[](uint32_t index) { return (reinterpret_cast<int32_t*>(&vec_))[index]; }
        int operator[](uint32_t index) const { return (const_cast<int32_t*>(reinterpret_cast<const int32_t*>(&vec_)))[index]; }
    };
    static VECTOR_INT ZERO_VEC = { _mm256_set_epi32(0, 0, 0, 0, 0, 0, 0, 0) };
    static VECTOR_INT ONE_VEC = { _mm256_set_epi32(1, 1, 1, 1, 1, 1, 1, 1) };


    struct VECTOR_BOOL {
        __attribute__ ((aligned (8))) __mmask8 mask_;
        bool& operator[](uint32_t index) { return (reinterpret_cast<bool*>(&mask_))[index]; }
        bool operator[](uint32_t index) const { return (const_cast<bool*>(reinterpret_cast<const bool*>(&mask_)))[index]; }
        VECTOR_BOOL& operator&=(VECTOR_BOOL mask) { mask_ &= mask.mask_; return *this; }
        VECTOR_BOOL& operator|=(VECTOR_BOOL mask) { mask_ |= mask.mask_; return *this; }
    };
    static VECTOR_BOOL TRUE_VEC = { 0xFF };
    static VECTOR_BOOL FALSE_VEC = { 0x0 };


    inline VECTOR_INT get_vec(int32_t value) {
        return { _mm256_set_epi32(value, value, value, value, value, value, value, value) };
    }


    // SIMD arithmetic
    inline VECTOR_INT operator+(VECTOR_INT vec1, VECTOR_INT vec2) { return { _mm256_add_epi32(vec1.vec_, vec2.vec_) }; }
    inline VECTOR_INT operator+(VECTOR_INT vec, int32_t value) { return vec + get_vec(value); }
    inline VECTOR_INT operator+(int32_t value, VECTOR_INT vec) { return get_vec(value) + vec; }

    inline VECTOR_INT operator-(VECTOR_INT vec1, VECTOR_INT vec2) { return { _mm256_sub_epi32(vec1.vec_, vec2.vec_) }; }
    inline VECTOR_INT operator-(VECTOR_INT vec, int32_t value) { return vec - get_vec(value); }
    inline VECTOR_INT operator-(int32_t value, VECTOR_INT vec) { return get_vec(value) - vec; }

    inline VECTOR_INT operator*(VECTOR_INT vec1, VECTOR_INT vec2) { return { _mm256_mul_epi32(vec1.vec_, vec2.vec_) }; }
    inline VECTOR_INT operator*(VECTOR_INT vec, int32_t value) { return vec * get_vec(value); }
    inline VECTOR_INT operator*(int32_t value, VECTOR_INT vec) { return get_vec(value) * vec; }


    // SIMD compare
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


} // end namespace DB::ap