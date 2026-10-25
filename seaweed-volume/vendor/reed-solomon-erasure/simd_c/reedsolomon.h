/* reedsolomon.h - SIMD-optimized Galois-field multiplication routines
 *
 * Copyright (c) 2015, 2016 Nicolas Trangez
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

#include <stdint.h>

#if HAVE_CONFIG_H
# include "config.h"
#endif

#define PROTO_RETURN size_t
#define PROTO_ARGS                              \
        const uint8_t low[16],                  \
        const uint8_t high[16],                 \
        const uint8_t *restrict const in,       \
        uint8_t *restrict const out,            \
        const size_t len
#define PROTO(name)                     \
        PROTO_RETURN                    \
        name (PROTO_ARGS)

PROTO(reedsolomon_gal_mul);
PROTO(reedsolomon_gal_mul_xor);

typedef enum {
        REEDSOLOMON_CPU_GENERIC = 0,
        REEDSOLOMON_CPU_SSE2 = 1,
        REEDSOLOMON_CPU_SSSE3 = 2,
        REEDSOLOMON_CPU_AVX = 3,
        REEDSOLOMON_CPU_AVX2 = 4,
        REEDSOLOMON_CPU_NEON = 5,
        REEDSOLOMON_CPU_ALTIVEC = 6,
} reedsolomon_cpu_support;

reedsolomon_cpu_support reedsolomon_determine_cpu_support(void);
