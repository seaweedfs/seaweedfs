package s3api

import "math"

// crcParams describes a CRC algorithm in the Rocksoft/Williams parameterization (see "15. A
// Parameterized Model For CRC Algorithms" in http://www.ross.net/crc/download/crc_v3.txt)
type crcParams struct {
	width  uint32 // the width of the algorithm expressed in bits; 1 less than the width of poly
	poly   uint64 // the unreflected poly
	init   uint64 // initial register value
	xorout uint64 // value XORed into the final register
	refin  bool   // reflect input bytes
	refout bool   // reflect final register
}

var crcCombineParams = map[ChecksumAlgorithm]crcParams{
	ChecksumAlgorithmCRC64NVMe: {
		width:  64,
		poly:   0xad93d23594c93659,
		init:   0xffffffffffffffff,
		xorout: 0xffffffffffffffff,
		refin:  true,
		refout: true,
	},
	ChecksumAlgorithmCRC32: {
		width:  32,
		poly:   0x04c11db7,
		init:   0xffffffff,
		xorout: 0xffffffff,
		refin:  true,
		refout: true,
	},
	ChecksumAlgorithmCRC32C: {
		width:  32,
		poly:   0x1edc6f41,
		init:   0xffffffff,
		xorout: 0xffffffff,
		refin:  true,
		refout: true,
	},
}

// Ported from: https://github.com/awesomized/crc-fast-rust/blob/3a853cc7daf2cd47cc4466f198680cabdfb0b5fa/src/combine.rs

/*
  Derived from this excellent answer by Mark Adler on StackOverflow:
  https://stackoverflow.com/questions/29915764/generic-crc-8-16-32-64-combine-implementation/29928573#29928573
*/

/* crccomb.c -- generalized combination of CRCs
 * Copyright (C) 2015 Mark Adler
 * Version 1.1  29 Apr 2015  Mark Adler
 */

/*
 This software is provided 'as-is', without any express or implied
 warranty.  In no event will the author be held liable for any damages
 arising from the use of this software.

 Permission is granted to anyone to use this software for any purpose,
 including commercial applications, and to alter it and redistribute it
 freely, subject to the following restrictions:

 1. The origin of this software must not be misrepresented; you must not
    claim that you wrote the original software. If you use this software
    in a product, an acknowledgment in the product documentation would be
    appreciated but is not required.
 2. Altered source versions must be plainly marked as such, and must not be
    misrepresented as being the original software.
 3. This notice may not be removed or altered from any source distribution.

 Mark Adler
 madler@alumni.caltech.edu
*/

/*
  zlib provides a fast operation to combine the CRCs of two sequences of bytes
  into a single CRC, which is the CRC of the two sequences concatenated.  That
  operation requires only the two CRC's and the length of the second sequence.
  The routine in zlib only works on the particular CRC-32 used by zlib.  The
  code provided here generalizes that operation to apply to a wide range of
  CRCs.  The CRC is specified in a series of #defines, based on the
  parameterization found in Ross William's excellent CRC tutorial here:

     http://www.ross.net/crc/download/crc_v3.txt

  A comprehensive catalogue of known CRCs, their parameters, check values, and
  references can be found here:

     http://reveng.sourceforge.net/crc-catalogue/all.htm
*/

// Multiply the GF(2) vector vec by the GF(2) matrix mat, returning the
// resulting vector.  The vector is stored as bits in a crc_t.  The matrix is
// similarly stored with each column as a crc_t, where the number of columns is
// at least enough to cover the position of the most significant 1 bit in the
// vector (so a dimension parameter is not needed).
func gf2MatrixTimes(mat *[64]uint64, vec uint64) uint64 {
	var sum uint64
	idx := 0

	for vec > 0 {
		if vec&1 == 1 {
			sum ^= mat[idx]
		}
		vec >>= 1
		idx++
	}

	return sum
}

// Multiply the matrix mat by itself, returning the result in square.  WIDTH is
// the dimension of the matrices, i.e., the number of bits in each crc_t
// (rows), and the number of crc_t's (columns).
func gf2MatrixSquare(square *[64]uint64, mat *[64]uint64) {
	for n := 0; n < 64; n++ {
		square[n] = gf2MatrixTimes(mat, mat[n])
	}
}

// Combine the CRCs of two successive sequences, where crc1 is the CRC of the
// first sequence of bytes, crc2 is the CRC of the immediately following
// sequence of bytes, and len2 is the length of the second sequence.  The CRC
// of the combined sequence is returned.
func combineCRC(crc1 uint64, crc2 uint64, len2 uint64, params crcParams) uint64 {
	even := [64]uint64{} /* even-power-of-two zeros operator */
	odd := [64]uint64{}  /* odd-power-of-two zeros operator */

	/* exclusive-or the result with len2 zeros applied to the CRC of an empty
	   sequence */
	crc1 ^= params.init ^ params.xorout

	/* construct the operator for one zero bit and put in odd[] */
	if params.refin && params.refout {
		// use the reflected POLY
		odd[0] = reflectPoly(params.poly, params.width)
		col := uint64(1)
		for n := uint32(1); n < params.width; n++ {
			odd[n] = col
			col <<= 1
		}
	} else if !params.refin && !params.refout {
		col := uint64(2)
		for n := uint32(0); n < params.width-1; n++ {
			odd[n] = col
			col <<= 1
		}
		odd[params.width-1] = params.poly
	} else {
		panic("Unsupported CRC configuration")
	}

	/* put operator for two zero bits in even */
	gf2MatrixSquare(&even, &odd)

	/* put operator for four zero bits in odd */
	gf2MatrixSquare(&odd, &even)

	/* apply len2 zeros to crc1 (first square will put the operator for one
	   zero byte, eight zero bits, in even) */
	for {
		/* apply zeros operator for this bit of len2 */
		gf2MatrixSquare(&even, &odd)
		if len2&1 == 1 {
			crc1 = gf2MatrixTimes(&even, crc1)
		}
		len2 >>= 1

		/* if no more bits set, then done */
		if len2 == 0 {
			break
		}

		/* another iteration of the loop with odd and even swapped */
		gf2MatrixSquare(&odd, &even)
		if len2&1 == 1 {
			crc1 = gf2MatrixTimes(&odd, crc1)
		}
		len2 >>= 1

		/* if no more bits set, then done */
		if len2 == 0 {
			break
		}
	}

	/* return combined crc */
	return crc1 ^ crc2
}

func reflectPoly(poly uint64, width uint32) uint64 {
	if width > 64 {
		panic("Width must be <= 64 bits")
	}

	// First reverse all bits
	reversed := bitReverse(poly)

	// Shift right to get the significant bits in the correct position
	// For a 32-bit poly, we need to shift right by (64 - 32) = 32 bits
	shifted := reversed >> (64 - width)

	// Create mask for the target width
	var mask uint64
	if width == 64 {
		mask = math.MaxUint64
	} else {
		mask = (1 << width) - 1
	}

	// Apply mask to ensure we only keep the bits we want
	return shifted & mask
}

func bitReverse(forward uint64) uint64 {
	reversed := uint64(0)

	for i := 0; i < 64; i++ {
		reversed <<= 1
		reversed |= forward & 1
		forward >>= 1
	}

	return reversed
}
