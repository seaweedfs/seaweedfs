/* reedsolomon.c - SIMD-optimized Galois-field multiplication routines
 *
 * Copyright (c) 2015, 2016 Nicolas Trangez
 * Copyright (c) 2015 Klaus Post
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE
 */

#if HAVE_CONFIG_H
# include "config.h"
#endif

#include <stdint.h>
#include <string.h>

//#if defined(__SSE2__) && __SSE2__ && defined(HAVE_EMMINTRIN_H) && HAVE_EMMINTRIN_H
//#ifdef __SSE2__
#if defined(__SSE2__) && __SSE2__
# define USE_SSE2 1
# undef VECTOR_SIZE
# define VECTOR_SIZE 16
# include <emmintrin.h>
#else
# define USE_SSE2 0
#endif

//#if defined(__SSSE3__) && __SSSE3__ && defined(HAVE_TMMINTRIN_H) && HAVE_TMMINTRIN_H
//#ifdef __SSSE3__
#if defined(__SSSE3__) && __SSSE3__
# define USE_SSSE3 1
# undef VECTOR_SIZE
# define VECTOR_SIZE 16
# include <tmmintrin.h>
#else
# define USE_SSSE3 0
#endif

//#if defined(__AVX2__) && __AVX2__ && defined(HAVE_IMMINTRIN_H) && HAVE_IMMINTRIN_H
//#ifdef __AVX2__
#if defined(__AVX2__) && __AVX2__
# define USE_AVX2 1
# undef VECTOR_SIZE
# define VECTOR_SIZE 32
# include <immintrin.h>
#else
# define USE_AVX2 0
#endif


#if defined(__AVX512F__) && __AVX512F__
# define USE_AVX512 1
# undef VECTOR_SIZE
# define VECTOR_SIZE 64
# include <immintrin.h>
#else
# define USE_AVX512 0
#endif


/*#if ((defined(__ARM_NEON__) && __ARM_NEON__) \
        || (defined(__ARM_NEON) && __ARM_NEON) \
        || (defined(__aarch64__) && __aarch64__)) \
        && defined(HAVE_ARM_NEON_H) && HAVE_ARM_NEON_H*/
#if ((defined(__ARM_NEON__) && __ARM_NEON__)      \
     || (defined(__ARM_NEON) && __ARM_NEON)       \
     || (defined(__aarch64__) && __aarch64__))
# define USE_ARM_NEON 1
#undef VECTOR_SIZE
# define VECTOR_SIZE 16
# include <arm_neon.h>
#else
# define USE_ARM_NEON 0
#endif

//#if defined(__ALTIVEC__) && __ALTIVEC__ && defined(HAVE_ALTIVEC_H) && HAVE_ALTIVEC_H
#if defined(__ALTIVEC__) && __ALTIVEC__
# define USE_ALTIVEC 1
# undef VECTOR_SIZE
# define VECTOR_SIZE 16
# include <altivec.h>
#else
# define USE_ALTIVEC 0
#endif

#ifndef VECTOR_SIZE
/* 'Generic' code */
# define VECTOR_SIZE 16
#endif

# define USE_ALIGNED_ACCESS 0
# define ALIGNED_ACCESS __attribute__((unused))
# define UNALIGNED_ACCESS

#include "reedsolomon.h"

#if defined(HAVE_FUNC_ATTRIBUTE_HOT) && HAVE_FUNC_ATTRIBUTE_HOT
# define HOT_FUNCTION   __attribute__((hot))
#else
# define HOT_FUNCTION
#endif

#if defined(HAVE_FUNC_ATTRIBUTE_CONST) && HAVE_FUNC_ATTRIBUTE_CONST
# define CONST_FUNCTION __attribute__((const))
#else
# define CONST_FUNCTION
#endif

#if defined(HAVE_FUNC_ATTRIBUTE_ALWAYS_INLINE) && HAVE_FUNC_ATTRIBUTE_ALWAYS_INLINE
# define ALWAYS_INLINE  inline __attribute__((always_inline))
#else
# define ALWAYS_INLINE  inline
#endif

#if defined(HAVE_FUNC_ATTRIBUTE_FORCE_ALIGN_ARG_POINTER) && HAVE_FUNC_ATTRIBUTE_FORCE_ALIGN_ARG_POINTER
# define FORCE_ALIGN_ARG_POINTER __attribute__((force_align_arg_pointer))
#else
# define FORCE_ALIGN_ARG_POINTER
#endif

#define CONCAT_HELPER(a, b)     a ## b
#define CONCAT(a, b)            CONCAT_HELPER(a, b)

typedef uint8_t v16u8v __attribute__((vector_size(16), aligned(1)));
typedef uint64_t v2u64v __attribute__((vector_size(16), aligned(1)));

#define T(t, n) t n[VSIZE / 8 / sizeof(t)]
#define T1(t, n) t n

#define VSIZE 128
typedef union {
        T(uint8_t, u8);
        T(uint64_t, u64);
#if USE_SSE2
        T1(__m128i, m128i);
#endif
#if USE_ARM_NEON
        T1(uint8x16_t, uint8x16);
        T1(uint8x8x2_t, uint8x8x2);
#endif
#if USE_ALTIVEC
        T1(__vector uint8_t, uint8x16);
        T1(__vector uint64_t, uint64x2);
#endif
        T1(v16u8v, v16u8);
        T1(v2u64v, v2u64);
} v128 __attribute__((aligned(1)));
#undef VSIZE

#define VSIZE 256
typedef union {
        T(uint8_t, u8);
#if USE_AVX2
        __m256i m256i;
#endif
} v256 __attribute__((aligned(1)));
#undef VSIZE

#define VSIZE 512
typedef union {
        T(uint8_t, u8);
#if USE_AVX512
        __m512i m512i;
#endif
} v512 __attribute__((aligned(1)));

#undef T
#undef T1

#if VECTOR_SIZE == 16
typedef v128 v;
#elif VECTOR_SIZE == 32
typedef v256 v;
#elif VECTOR_SIZE == 64
typedef v512 v;
#else
# error Unsupported VECTOR_SIZE
#endif

static ALWAYS_INLINE UNALIGNED_ACCESS v128 loadu_v128(const uint8_t *in) {
#if USE_SSE2
        const v128 result = { .m128i = _mm_loadu_si128((const __m128i *)in) };
#else
        v128 result;
        memcpy(&result.u64, in, sizeof(result.u64));
#endif

        return result;
}

static ALWAYS_INLINE UNALIGNED_ACCESS v loadu_v(const uint8_t *in) {
#if USE_AVX512
        const v512 result = { .m512i = _mm512_loadu_si512((const __m512i *)in) };
#elif USE_AVX2
        const v256 result = { .m256i = _mm256_loadu_si256((const __m256i *)in) };
#else
        const v128 result = loadu_v128(in);
#endif

        return result;
}

static ALWAYS_INLINE ALIGNED_ACCESS v load_v(const uint8_t *in) {
#if USE_AVX512
        const v512 result = { .m512i = _mm512_load_si512((const __m512i *)in) };
#elif USE_AVX2
        const v256 result = { .m256i = _mm256_load_si256((const __m256i *)in) };
#elif USE_SSE2
        const v128 result = { .m128i = _mm_load_si128((const __m128i *)in) };
#elif USE_ARM_NEON
        const v128 result = { .uint8x16 = vld1q_u8(in) };
#elif USE_ALTIVEC
        const v128 result = { .uint8x16 = vec_ld(0, in) };
#else
        const v128 result = loadu_v128(in);
#endif

        return result;
}

static ALWAYS_INLINE CONST_FUNCTION v set1_epi8_v(const uint8_t c) {
#if USE_AVX512
        const v512 result = { .m512i = _mm512_set1_epi8(c) };
#elif USE_AVX2
        const v256 result = { .m256i = _mm256_set1_epi8(c) };
#elif USE_SSE2
        const v128 result = { .m128i = _mm_set1_epi8(c) };
#elif USE_ARM_NEON
        const v128 result = { .uint8x16 = vdupq_n_u8(c) };
#elif USE_ALTIVEC
        const v128 result = { .uint8x16 = { c, c, c, c, c, c, c, c,
                                            c, c, c, c, c, c, c, c } };
#else
        uint64_t c2 = c,
                 tmp = (c2 << (7 * 8)) |
                       (c2 << (6 * 8)) |
                       (c2 << (5 * 8)) |
                       (c2 << (4 * 8)) |
                       (c2 << (3 * 8)) |
                       (c2 << (2 * 8)) |
                       (c2 << (1 * 8)) |
                       (c2 << (0 * 8));
        const v128 result = { .u64 = { tmp, tmp } };
#endif

        return result;
}

static ALWAYS_INLINE CONST_FUNCTION v srli_epi64_v(const v in /*, const unsigned int n*/) {
        // TODO: Hard code n to 4 to avoid build issues on M1 Macs (the
        //       `USE_ARM_NEON` path below) where apple clang is failing to
        //       recognize the constant `n`.
        //
        //       See https://github.com/rust-rse/reed-solomon-erasure/pull/92
        //
        #define n 4
#if USE_AVX512
        const v512 result = { .m512i = _mm512_srli_epi64(in.m512i, n) };
#elif USE_AVX2
        const v256 result = { .m256i = _mm256_srli_epi64(in.m256i, n) };
#elif USE_SSE2
        const v128 result = { .m128i = _mm_srli_epi64(in.m128i, n) };
#elif USE_ARM_NEON
        const v128 result = { .uint8x16 = vshrq_n_u8(in.uint8x16, n) };
#elif USE_ALTIVEC
# if RS_HAVE_VEC_VSRD
        const v128 shift = { .v2u64 = { n, n } },
                   result = { .uint64x2 = vec_vsrd(in.v2u64, shift.v2u64) };
# else
        const v128 result = { .v2u64 = in.v2u64 >> n };
# endif
#else
        const v128 result = { .u64 = { in.u64[0] >> n,
                                       in.u64[1] >> n } };
#endif
        #undef n
        return result;
}

static ALWAYS_INLINE CONST_FUNCTION v and_v(const v a, const v b) {
#if USE_AVX512
        const v512 result = { .m512i = _mm512_and_si512(a.m512i, b.m512i) };
#elif USE_AVX2
        const v256 result = { .m256i = _mm256_and_si256(a.m256i, b.m256i) };
#elif USE_SSE2
        const v128 result = { .m128i = _mm_and_si128(a.m128i, b.m128i) };
#elif USE_ARM_NEON
        const v128 result = { .uint8x16 = vandq_u8(a.uint8x16, b.uint8x16) };
#elif USE_ALTIVEC
        const v128 result = { .uint8x16 = vec_and(a.uint8x16, b.uint8x16) };
#else
        const v128 result = { .v2u64 = a.v2u64 & b.v2u64 };
#endif

        return result;
}

static ALWAYS_INLINE CONST_FUNCTION v xor_v(const v a, const v b) {
#if USE_AVX512
        const v512 result = { .m512i = _mm512_xor_si512(a.m512i, b.m512i) };
#elif USE_AVX2
        const v256 result = { .m256i = _mm256_xor_si256(a.m256i, b.m256i) };
#elif USE_SSE2
        const v128 result = { .m128i = _mm_xor_si128(a.m128i, b.m128i) };
#elif USE_ARM_NEON
        const v128 result = { .uint8x16 = veorq_u8(a.uint8x16, b.uint8x16) };
#elif USE_ALTIVEC
        const v128 result = { .uint8x16 = vec_xor(a.uint8x16, b.uint8x16) };
#else
        const v128 result = { .v2u64 = a.v2u64 ^ b.v2u64 };
#endif

        return result;
}

static ALWAYS_INLINE CONST_FUNCTION v shuffle_epi8_v(const v vec, const v mask) {
#if USE_AVX512
        const v512 result = { .m512i = _mm512_shuffle_epi8(vec.m512i, mask.m512i) };
#elif USE_AVX2
        const v256 result = { .m256i = _mm256_shuffle_epi8(vec.m256i, mask.m256i) };
#elif USE_SSSE3
        const v128 result = { .m128i = _mm_shuffle_epi8(vec.m128i, mask.m128i) };
#elif USE_ARM_NEON
# if defined(RS_HAVE_VQTBL1Q_U8) && RS_HAVE_VQTBL1Q_U8
        const v128 result = { .uint8x16 = vqtbl1q_u8(vec.uint8x16, mask.uint8x16) };
# else
        /* There's no NEON instruction mapping 1-to-1 to _mm_shuffle_epi8, but
         * this should have the same result...
         */
        const v128 result = { .uint8x16 = vcombine_u8(vtbl2_u8(vec.uint8x8x2,
                                                               vget_low_u8(mask.uint8x16)),
                                                      vtbl2_u8(vec.uint8x8x2,
                                                               vget_high_u8(mask.uint8x16))) };

# endif
#elif USE_ALTIVEC
        const v128 zeros = set1_epi8_v(0),
                   result = { .uint8x16 = vec_perm(vec.uint8x16, zeros.uint8x16, mask.uint8x16) };
#elif defined(RS_HAVE_BUILTIN_SHUFFLE) && RS_HAVE_BUILTIN_SHUFFLE
        const v16u8v zeros = { 0, 0, 0, 0, 0, 0, 0, 0
                             , 0, 0, 0, 0, 0, 0, 0, 0 };
        const v128 result = { .v16u8 = __builtin_shuffle(vec.v16u8, zeros, mask.v16u8) };
#else
        v128 result = { .u64 = { 0, 0 } };

# define DO_BYTE(i) \
        result.u8[i] = mask.u8[i] & 0x80 ? 0 : vec.u8[mask.u8[i] & 0x0F];

        DO_BYTE( 0); DO_BYTE( 1); DO_BYTE( 2); DO_BYTE( 3);
        DO_BYTE( 4); DO_BYTE( 5); DO_BYTE( 6); DO_BYTE( 7);
        DO_BYTE( 8); DO_BYTE( 9); DO_BYTE(10); DO_BYTE(11);
        DO_BYTE(12); DO_BYTE(13); DO_BYTE(14); DO_BYTE(15);
#endif

        return result;
}

static ALWAYS_INLINE UNALIGNED_ACCESS void storeu_v(uint8_t *out, const v vec) {
#if USE_AVX512
        _mm512_storeu_si512((__m512i *)out, vec.m512i);
#elif USE_AVX2
        _mm256_storeu_si256((__m256i *)out, vec.m256i);
#elif USE_SSE2
        _mm_storeu_si128((__m128i *)out, vec.m128i);
#else
        memcpy(out, &vec.u64, sizeof(vec.u64));
#endif
}

static ALWAYS_INLINE ALIGNED_ACCESS void store_v(uint8_t *out, const v vec) {
#if USE_AVX512
        _mm512_store_si512((__m512i *)out, vec.m512i);
#elif USE_AVX2
        _mm256_store_si256((__m256i *)out, vec.m256i);
#elif USE_SSE2
        _mm_store_si128((__m128i *)out, vec.m128i);
#elif USE_ARM_NEON
        vst1q_u8(out, vec.uint8x16);
#elif USE_ALTIVEC
        vec_st(vec.uint8x16, 0, out);
#else
        storeu_v(out, vec);
#endif
}

static ALWAYS_INLINE CONST_FUNCTION v replicate_v128_v(const v128 vec) {
#if USE_AVX512
        const v512 result = { .m512i = _mm512_broadcast_i32x4(vec.m128i) };
#elif USE_AVX2
        const v256 result = { .m256i = _mm256_broadcastsi128_si256(vec.m128i) };
#else
        const v128 result = vec;
#endif

        return result;
}


//+build !noasm !appengine

// Copyright 2015, Klaus Post, see LICENSE for details.

// Based on http://www.snia.org/sites/default/files2/SDC2013/presentations/NewThinking/EthanMiller_Screaming_Fast_Galois_Field%20Arithmetic_SIMD%20Instructions.pdf
// and http://jerasure.org/jerasure/gf-complete/tree/master

/*
// func galMulSSSE3Xor(low, high, in, out []byte)
TEXT ·galMulSSSE3Xor(SB), 7, $0
    MOVQ    low+0(FP),SI        // SI: &low
    MOVQ    high+24(FP),DX      // DX: &high
    MOVOU  (SI), X6             // X6 low
    MOVOU  (DX), X7             // X7: high
    MOVQ    $15, BX             // BX: low mask
    MOVQ    BX, X8
    PXOR    X5, X5
    MOVQ    in+48(FP),SI        // R11: &in
    MOVQ    in_len+56(FP),R9    // R9: len(in)
    MOVQ    out+72(FP), DX      // DX: &out
    PSHUFB  X5, X8              // X8: lomask (unpacked)
    SHRQ    $4, R9              // len(in) / 16
    CMPQ    R9 ,$0
    JEQ     done_xor
loopback_xor:
    MOVOU  (SI),X0   // in[x]
    MOVOU  (DX),X4   // out[x]
    MOVOU   X0, X1   // in[x]
    MOVOU   X6, X2   // low copy
    MOVOU   X7, X3   // high copy
    PSRLQ   $4, X1   // X1: high input
    PAND    X8, X0   // X0: low input
    PAND    X8, X1   // X0: high input
    PSHUFB  X0, X2   // X2: mul low part
    PSHUFB  X1, X3   // X3: mul high part
    PXOR    X2, X3   // X3: Result
    PXOR    X4, X3   // X3: Result xor existing out
    MOVOU   X3, (DX) // Store
    ADDQ    $16, SI  // in+=16
    ADDQ    $16, DX  // out+=16
    SUBQ    $1, R9
    JNZ     loopback_xor
done_xor:
    RET

// func galMulSSSE3(low, high, in, out []byte)
TEXT ·galMulSSSE3(SB), 7, $0
    MOVQ    low+0(FP),SI        // SI: &low
    MOVQ    high+24(FP),DX      // DX: &high
    MOVOU   (SI), X6            // X6 low
    MOVOU   (DX), X7            // X7: high
    MOVQ    $15, BX             // BX: low mask
    MOVQ    BX, X8
    PXOR    X5, X5
    MOVQ    in+48(FP),SI        // R11: &in
    MOVQ    in_len+56(FP),R9    // R9: len(in)
    MOVQ    out+72(FP), DX      // DX: &out
    PSHUFB  X5, X8              // X8: lomask (unpacked)
    SHRQ    $4, R9              // len(in) / 16
    CMPQ    R9 ,$0
    JEQ     done
loopback:
    MOVOU  (SI),X0   // in[x]
    MOVOU   X0, X1   // in[x]
    MOVOU   X6, X2   // low copy
    MOVOU   X7, X3   // high copy
    PSRLQ   $4, X1   // X1: high input
    PAND    X8, X0   // X0: low input
    PAND    X8, X1   // X0: high input
    PSHUFB  X0, X2   // X2: mul low part
    PSHUFB  X1, X3   // X3: mul high part
    PXOR    X2, X3   // X3: Result
    MOVOU   X3, (DX) // Store
    ADDQ    $16, SI  // in+=16
    ADDQ    $16, DX  // out+=16
    SUBQ    $1, R9
    JNZ     loopback
done:
    RET
*/

static ALWAYS_INLINE v reedsolomon_gal_mul_v(
        const v low_mask_unpacked,
        const v low_vector,
        const v high_vector,

        v (*modifier)(const v new, const v old),

        const v in_x,
        const v old) {
        const v low_input = and_v(in_x, low_mask_unpacked),
                in_x_shifted = srli_epi64_v(in_x /*, 4*/),
                high_input = and_v(in_x_shifted, low_mask_unpacked),

                mul_low_part = shuffle_epi8_v(low_vector, low_input),
                mul_high_part = shuffle_epi8_v(high_vector, high_input),

                new = xor_v(mul_low_part, mul_high_part),
                result = modifier(new, old);

        return result;
}

static ALWAYS_INLINE PROTO_RETURN reedsolomon_gal_mul_impl(
        PROTO_ARGS,
        v (*modifier)(const v new, const v old)) {
        const v low_mask_unpacked = set1_epi8_v(0x0f);

        const v128 low_vector128 = loadu_v128(low),
                   high_vector128 = loadu_v128(high);
        const v low_vector = replicate_v128_v(low_vector128),
                high_vector = replicate_v128_v(high_vector128);

        size_t done = 0;

#if USE_ALIGNED_ACCESS
# define LOAD(addr) load_v(addr)
# define STORE(addr, vec) store_v(addr, vec)
#else
# define LOAD(addr) loadu_v(addr)
# define STORE(addr, vec) storeu_v(addr, vec)
#endif

#if RS_HAVE_CLANG_LOOP_UNROLL
# pragma clang loop unroll(enable)
#endif
        for(size_t x = 0; x < len / sizeof(v); x++) {
                const v in_x = LOAD(&in[done]),
                        old = LOAD(&out[done]),
                        result = reedsolomon_gal_mul_v(
                                        low_mask_unpacked,
                                        low_vector, high_vector,
                                        modifier,
                                        in_x,
                                        old);

                STORE(&out[done], result);

                done += sizeof(v);
        }

        return done;
}

static ALWAYS_INLINE CONST_FUNCTION v noop(const v new, const v old __attribute__((__unused__))) {
        return new;
}

#ifdef HOT
HOT_FUNCTION
#endif
FORCE_ALIGN_ARG_POINTER PROTO(reedsolomon_gal_mul) {
        return reedsolomon_gal_mul_impl(low, high, in, out, len, noop);
}

#ifdef HOT
HOT_FUNCTION
#endif
FORCE_ALIGN_ARG_POINTER PROTO(reedsolomon_gal_mul_xor) {
        return reedsolomon_gal_mul_impl(low, high, in, out, len, xor_v);
}
