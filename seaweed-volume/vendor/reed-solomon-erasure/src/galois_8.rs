//! Implementation of GF(2^8): the finite field with 2^8 elements.

include!(concat!(env!("OUT_DIR"), "/table.rs"));

/// The field GF(2^8).
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq)]
pub struct Field;

impl crate::Field for Field {
    const ORDER: usize = 256;
    type Elem = u8;

    fn add(a: u8, b: u8) -> u8 {
        add(a, b)
    }

    fn mul(a: u8, b: u8) -> u8 {
        mul(a, b)
    }

    fn div(a: u8, b: u8) -> u8 {
        div(a, b)
    }

    fn exp(elem: u8, n: usize) -> u8 {
        exp(elem, n)
    }

    fn zero() -> u8 {
        0
    }

    fn one() -> u8 {
        1
    }

    fn nth_internal(n: usize) -> u8 {
        n as u8
    }

    fn mul_slice(c: u8, input: &[u8], out: &mut [u8]) {
        mul_slice(c, input, out)
    }

    fn mul_slice_add(c: u8, input: &[u8], out: &mut [u8]) {
        mul_slice_xor(c, input, out)
    }
}

/// Type alias of ReedSolomon over GF(2^8).
pub type ReedSolomon = crate::ReedSolomon<Field>;

/// Type alias of ShardByShard over GF(2^8).
pub type ShardByShard<'a> = crate::ShardByShard<'a, Field>;

/// Add two elements.
pub fn add(a: u8, b: u8) -> u8 {
    a ^ b
}

/// Subtract `b` from `a`.
#[cfg(test)]
pub fn sub(a: u8, b: u8) -> u8 {
    a ^ b
}

/// Multiply two elements.
pub fn mul(a: u8, b: u8) -> u8 {
    MUL_TABLE[a as usize][b as usize]
}

/// Divide one element by another. `b`, the divisor, may not be 0.
pub fn div(a: u8, b: u8) -> u8 {
    if a == 0 {
        0
    } else if b == 0 {
        panic!("Divisor is 0")
    } else {
        let log_a = LOG_TABLE[a as usize];
        let log_b = LOG_TABLE[b as usize];
        let mut log_result = log_a as isize - log_b as isize;
        if log_result < 0 {
            log_result += 255;
        }
        EXP_TABLE[log_result as usize]
    }
}

/// Compute a^n.
pub fn exp(a: u8, n: usize) -> u8 {
    if n == 0 {
        1
    } else if a == 0 {
        0
    } else {
        let log_a = LOG_TABLE[a as usize];
        let mut log_result = log_a as usize * n;
        while 255 <= log_result {
            log_result -= 255;
        }
        EXP_TABLE[log_result]
    }
}

const PURE_RUST_UNROLL: isize = 4;

macro_rules! return_if_empty {
    (
        $len:expr
    ) => {
        if $len == 0 {
            return;
        }
    };
}

#[cfg(not(all(
    feature = "simd-accel",
    any(target_arch = "x86_64", target_arch = "aarch64"),
    not(target_env = "msvc"),
    not(any(target_os = "android", target_os = "ios"))
)))]
pub fn mul_slice(c: u8, input: &[u8], out: &mut [u8]) {
    mul_slice_pure_rust(c, input, out);
}

#[cfg(not(all(
    feature = "simd-accel",
    any(target_arch = "x86_64", target_arch = "aarch64"),
    not(target_env = "msvc"),
    not(any(target_os = "android", target_os = "ios"))
)))]
pub fn mul_slice_xor(c: u8, input: &[u8], out: &mut [u8]) {
    mul_slice_xor_pure_rust(c, input, out);
}

fn mul_slice_pure_rust(c: u8, input: &[u8], out: &mut [u8]) {
    let mt = &MUL_TABLE[c as usize];
    let mt_ptr: *const u8 = &mt[0];

    assert_eq!(input.len(), out.len());

    let len: isize = input.len() as isize;
    return_if_empty!(len);

    let mut input_ptr: *const u8 = &input[0];
    let mut out_ptr: *mut u8 = &mut out[0];

    let mut n: isize = 0;
    unsafe {
        assert_eq!(4, PURE_RUST_UNROLL);
        if len > PURE_RUST_UNROLL {
            let len_minus_unroll = len - PURE_RUST_UNROLL;
            while n < len_minus_unroll {
                *out_ptr = *mt_ptr.offset(*input_ptr as isize);
                *out_ptr.offset(1) = *mt_ptr.offset(*input_ptr.offset(1) as isize);
                *out_ptr.offset(2) = *mt_ptr.offset(*input_ptr.offset(2) as isize);
                *out_ptr.offset(3) = *mt_ptr.offset(*input_ptr.offset(3) as isize);

                input_ptr = input_ptr.offset(PURE_RUST_UNROLL);
                out_ptr = out_ptr.offset(PURE_RUST_UNROLL);
                n += PURE_RUST_UNROLL;
            }
        }
        while n < len {
            *out_ptr = *mt_ptr.offset(*input_ptr as isize);

            input_ptr = input_ptr.offset(1);
            out_ptr = out_ptr.offset(1);
            n += 1;
        }
    }
    /* for n in 0..input.len() {
     *   out[n] = mt[input[n] as usize]
     * }
     */
}

fn mul_slice_xor_pure_rust(c: u8, input: &[u8], out: &mut [u8]) {
    let mt = &MUL_TABLE[c as usize];
    let mt_ptr: *const u8 = &mt[0];

    assert_eq!(input.len(), out.len());

    let len: isize = input.len() as isize;
    return_if_empty!(len);

    let mut input_ptr: *const u8 = &input[0];
    let mut out_ptr: *mut u8 = &mut out[0];

    let mut n: isize = 0;
    unsafe {
        assert_eq!(4, PURE_RUST_UNROLL);
        if len > PURE_RUST_UNROLL {
            let len_minus_unroll = len - PURE_RUST_UNROLL;
            while n < len_minus_unroll {
                *out_ptr ^= *mt_ptr.offset(*input_ptr as isize);
                *out_ptr.offset(1) ^= *mt_ptr.offset(*input_ptr.offset(1) as isize);
                *out_ptr.offset(2) ^= *mt_ptr.offset(*input_ptr.offset(2) as isize);
                *out_ptr.offset(3) ^= *mt_ptr.offset(*input_ptr.offset(3) as isize);

                input_ptr = input_ptr.offset(PURE_RUST_UNROLL);
                out_ptr = out_ptr.offset(PURE_RUST_UNROLL);
                n += PURE_RUST_UNROLL;
            }
        }
        while n < len {
            *out_ptr ^= *mt_ptr.offset(*input_ptr as isize);

            input_ptr = input_ptr.offset(1);
            out_ptr = out_ptr.offset(1);
            n += 1;
        }
    }
    /* for n in 0..input.len() {
     *   out[n] ^= mt[input[n] as usize];
     * }
     */
}

#[cfg(test)]
fn slice_xor(input: &[u8], out: &mut [u8]) {
    assert_eq!(input.len(), out.len());

    let len: isize = input.len() as isize;
    return_if_empty!(len);

    let mut input_ptr: *const u8 = &input[0];
    let mut out_ptr: *mut u8 = &mut out[0];

    let mut n: isize = 0;
    unsafe {
        assert_eq!(4, PURE_RUST_UNROLL);
        if len > PURE_RUST_UNROLL {
            let len_minus_unroll = len - PURE_RUST_UNROLL;
            while n < len_minus_unroll {
                *out_ptr ^= *input_ptr;
                *out_ptr.offset(1) ^= *input_ptr.offset(1);
                *out_ptr.offset(2) ^= *input_ptr.offset(2);
                *out_ptr.offset(3) ^= *input_ptr.offset(3);

                input_ptr = input_ptr.offset(PURE_RUST_UNROLL);
                out_ptr = out_ptr.offset(PURE_RUST_UNROLL);
                n += PURE_RUST_UNROLL;
            }
        }
        while n < len {
            *out_ptr ^= *input_ptr;

            input_ptr = input_ptr.offset(1);
            out_ptr = out_ptr.offset(1);
            n += 1;
        }
    }
    /* for n in 0..input.len() {
     *   out[n] ^= input[n]
     * }
     */
}

#[cfg(all(
    feature = "simd-accel",
    any(target_arch = "x86_64", target_arch = "aarch64"),
    not(target_env = "msvc"),
    not(any(target_os = "android", target_os = "ios"))
))]
extern "C" {
    fn reedsolomon_gal_mul(
        low: *const u8,
        high: *const u8,
        input: *const u8,
        out: *mut u8,
        len: libc::size_t,
    ) -> libc::size_t;

    fn reedsolomon_gal_mul_xor(
        low: *const u8,
        high: *const u8,
        input: *const u8,
        out: *mut u8,
        len: libc::size_t,
    ) -> libc::size_t;
}

#[cfg(all(
    feature = "simd-accel",
    any(target_arch = "x86_64", target_arch = "aarch64"),
    not(target_env = "msvc"),
    not(any(target_os = "android", target_os = "ios"))
))]
pub fn mul_slice(c: u8, input: &[u8], out: &mut [u8]) {
    let low: *const u8 = &MUL_TABLE_LOW[c as usize][0];
    let high: *const u8 = &MUL_TABLE_HIGH[c as usize][0];

    assert_eq!(input.len(), out.len());

    let input_ptr: *const u8 = &input[0];
    let out_ptr: *mut u8 = &mut out[0];
    let size: libc::size_t = input.len();

    let bytes_done: usize =
        unsafe { reedsolomon_gal_mul(low, high, input_ptr, out_ptr, size) as usize };

    mul_slice_pure_rust(c, &input[bytes_done..], &mut out[bytes_done..]);
}

#[cfg(all(
    feature = "simd-accel",
    any(target_arch = "x86_64", target_arch = "aarch64"),
    not(target_env = "msvc"),
    not(any(target_os = "android", target_os = "ios"))
))]
pub fn mul_slice_xor(c: u8, input: &[u8], out: &mut [u8]) {
    let low: *const u8 = &MUL_TABLE_LOW[c as usize][0];
    let high: *const u8 = &MUL_TABLE_HIGH[c as usize][0];

    assert_eq!(input.len(), out.len());

    let input_ptr: *const u8 = &input[0];
    let out_ptr: *mut u8 = &mut out[0];
    let size: libc::size_t = input.len();

    let bytes_done: usize =
        unsafe { reedsolomon_gal_mul_xor(low, high, input_ptr, out_ptr, size) as usize };

    mul_slice_xor_pure_rust(c, &input[bytes_done..], &mut out[bytes_done..]);
}

#[cfg(test)]
mod tests {
    extern crate alloc;

    use alloc::vec;

    use super::*;
    use crate::tests::fill_random;
    use rand;

    static BACKBLAZE_LOG_TABLE: [u8; 256] = [
        //-1,    0,    1,   25,    2,   50,   26,  198,
        // first value is changed from -1 to 0
        0, 0, 1, 25, 2, 50, 26, 198, 3, 223, 51, 238, 27, 104, 199, 75, 4, 100, 224, 14, 52, 141,
        239, 129, 28, 193, 105, 248, 200, 8, 76, 113, 5, 138, 101, 47, 225, 36, 15, 33, 53, 147,
        142, 218, 240, 18, 130, 69, 29, 181, 194, 125, 106, 39, 249, 185, 201, 154, 9, 120, 77,
        228, 114, 166, 6, 191, 139, 98, 102, 221, 48, 253, 226, 152, 37, 179, 16, 145, 34, 136, 54,
        208, 148, 206, 143, 150, 219, 189, 241, 210, 19, 92, 131, 56, 70, 64, 30, 66, 182, 163,
        195, 72, 126, 110, 107, 58, 40, 84, 250, 133, 186, 61, 202, 94, 155, 159, 10, 21, 121, 43,
        78, 212, 229, 172, 115, 243, 167, 87, 7, 112, 192, 247, 140, 128, 99, 13, 103, 74, 222,
        237, 49, 197, 254, 24, 227, 165, 153, 119, 38, 184, 180, 124, 17, 68, 146, 217, 35, 32,
        137, 46, 55, 63, 209, 91, 149, 188, 207, 205, 144, 135, 151, 178, 220, 252, 190, 97, 242,
        86, 211, 171, 20, 42, 93, 158, 132, 60, 57, 83, 71, 109, 65, 162, 31, 45, 67, 216, 183,
        123, 164, 118, 196, 23, 73, 236, 127, 12, 111, 246, 108, 161, 59, 82, 41, 157, 85, 170,
        251, 96, 134, 177, 187, 204, 62, 90, 203, 89, 95, 176, 156, 169, 160, 81, 11, 245, 22, 235,
        122, 117, 44, 215, 79, 174, 213, 233, 230, 231, 173, 232, 116, 214, 244, 234, 168, 80, 88,
        175,
    ];

    #[test]
    fn log_table_same_as_backblaze() {
        for i in 0..256 {
            assert_eq!(LOG_TABLE[i], BACKBLAZE_LOG_TABLE[i]);
        }
    }

    #[test]
    fn test_associativity() {
        for a in 0..256 {
            let a = a as u8;
            for b in 0..256 {
                let b = b as u8;
                for c in 0..256 {
                    let c = c as u8;
                    let x = add(a, add(b, c));
                    let y = add(add(a, b), c);
                    assert_eq!(x, y);
                    let x = mul(a, mul(b, c));
                    let y = mul(mul(a, b), c);
                    assert_eq!(x, y);
                }
            }
        }
    }

    quickcheck! {
        fn qc_add_associativity(a: u8, b: u8, c: u8) -> bool {
            add(a, add(b, c)) == add(add(a, b), c)
        }

        fn qc_mul_associativity(a: u8, b: u8, c: u8) -> bool {
            mul(a, mul(b, c)) == mul(mul(a, b), c)
        }
    }

    #[test]
    fn test_identity() {
        for a in 0..256 {
            let a = a as u8;
            let b = sub(0, a);
            let c = sub(a, b);
            assert_eq!(c, 0);
            if a != 0 {
                let b = div(1, a);
                let c = mul(a, b);
                assert_eq!(c, 1);
            }
        }
    }

    quickcheck! {
        fn qc_additive_identity(a: u8) -> bool {
            sub(a, sub(0, a)) == 0
        }

        fn qc_multiplicative_identity(a: u8) -> bool {
            if a == 0 { true }
            else      { mul(a, div(1, a)) == 1 }
        }
    }

    #[test]
    fn test_commutativity() {
        for a in 0..256 {
            let a = a as u8;
            for b in 0..256 {
                let b = b as u8;
                let x = add(a, b);
                let y = add(b, a);
                assert_eq!(x, y);
                let x = mul(a, b);
                let y = mul(b, a);
                assert_eq!(x, y);
            }
        }
    }

    quickcheck! {
        fn qc_add_commutativity(a: u8, b: u8) -> bool {
            add(a, b) == add(b, a)
        }

        fn qc_mul_commutativity(a: u8, b: u8) -> bool {
            mul(a, b) == mul(b, a)
        }
    }

    #[test]
    fn test_distributivity() {
        for a in 0..256 {
            let a = a as u8;
            for b in 0..256 {
                let b = b as u8;
                for c in 0..256 {
                    let c = c as u8;
                    let x = mul(a, add(b, c));
                    let y = add(mul(a, b), mul(a, c));
                    assert_eq!(x, y);
                }
            }
        }
    }

    quickcheck! {
        fn qc_add_distributivity(a: u8, b: u8, c: u8) -> bool {
            mul(a, add(b, c)) == add(mul(a, b), mul(a, c))
        }
    }

    #[test]
    fn test_exp() {
        for a in 0..256 {
            let a = a as u8;
            let mut power = 1u8;
            for j in 0..256 {
                let x = exp(a, j);
                assert_eq!(x, power);
                power = mul(power, a);
            }
        }
    }

    #[test]
    fn test_galois() {
        assert_eq!(mul(3, 4), 12);
        assert_eq!(mul(7, 7), 21);
        assert_eq!(mul(23, 45), 41);

        let input = [
            0, 1, 2, 3, 4, 5, 6, 10, 50, 100, 150, 174, 201, 255, 99, 32, 67, 85, 200, 199, 198,
            197, 196, 195, 194, 193, 192, 191, 190, 189, 188, 187, 186, 185,
        ];
        let mut output1 = vec![0; input.len()];
        let mut output2 = vec![0; input.len()];
        mul_slice(25, &input, &mut output1);
        let expect = [
            0x0, 0x19, 0x32, 0x2b, 0x64, 0x7d, 0x56, 0xfa, 0xb8, 0x6d, 0xc7, 0x85, 0xc3, 0x1f,
            0x22, 0x7, 0x25, 0xfe, 0xda, 0x5d, 0x44, 0x6f, 0x76, 0x39, 0x20, 0xb, 0x12, 0x11, 0x8,
            0x23, 0x3a, 0x75, 0x6c, 0x47,
        ];
        for i in 0..input.len() {
            assert_eq!(expect[i], output1[i]);
        }
        mul_slice(25, &input, &mut output2);
        for i in 0..input.len() {
            assert_eq!(expect[i], output2[i]);
        }

        let expect_xor = [
            0x0, 0x2d, 0x5a, 0x77, 0xb4, 0x99, 0xee, 0x2f, 0x79, 0xf2, 0x7, 0x51, 0xd4, 0x19, 0x31,
            0xc9, 0xf8, 0xfc, 0xf9, 0x4f, 0x62, 0x15, 0x38, 0xfb, 0xd6, 0xa1, 0x8c, 0x96, 0xbb,
            0xcc, 0xe1, 0x22, 0xf, 0x78,
        ];
        mul_slice_xor(52, &input, &mut output1);
        for i in 0..input.len() {
            assert_eq!(expect_xor[i], output1[i]);
        }
        mul_slice_xor(52, &input, &mut output2);
        for i in 0..input.len() {
            assert_eq!(expect_xor[i], output2[i]);
        }

        let expect = [
            0x0, 0xb1, 0x7f, 0xce, 0xfe, 0x4f, 0x81, 0x9e, 0x3, 0x6, 0xe8, 0x75, 0xbd, 0x40, 0x36,
            0xa3, 0x95, 0xcb, 0xc, 0xdd, 0x6c, 0xa2, 0x13, 0x23, 0x92, 0x5c, 0xed, 0x1b, 0xaa,
            0x64, 0xd5, 0xe5, 0x54, 0x9a,
        ];
        mul_slice(177, &input, &mut output1);
        for i in 0..input.len() {
            assert_eq!(expect[i], output1[i]);
        }
        mul_slice(177, &input, &mut output2);
        for i in 0..input.len() {
            assert_eq!(expect[i], output2[i]);
        }

        let expect_xor = [
            0x0, 0xc4, 0x95, 0x51, 0x37, 0xf3, 0xa2, 0xfb, 0xec, 0xc5, 0xd0, 0xc7, 0x53, 0x88,
            0xa3, 0xa5, 0x6, 0x78, 0x97, 0x9f, 0x5b, 0xa, 0xce, 0xa8, 0x6c, 0x3d, 0xf9, 0xdf, 0x1b,
            0x4a, 0x8e, 0xe8, 0x2c, 0x7d,
        ];
        mul_slice_xor(117, &input, &mut output1);
        for i in 0..input.len() {
            assert_eq!(expect_xor[i], output1[i]);
        }
        mul_slice_xor(117, &input, &mut output2);
        for i in 0..input.len() {
            assert_eq!(expect_xor[i], output2[i]);
        }

        assert_eq!(exp(2, 2), 4);
        assert_eq!(exp(5, 20), 235);
        assert_eq!(exp(13, 7), 43);
    }

    #[test]
    fn test_slice_add() {
        let length_list = [16, 32, 34];
        for len in length_list.iter() {
            let mut input = vec![0; *len];
            fill_random(&mut input);
            let mut output = vec![0; *len];
            fill_random(&mut output);
            let mut expect = vec![0; *len];
            for i in 0..expect.len() {
                expect[i] = input[i] ^ output[i];
            }
            slice_xor(&input, &mut output);
            for i in 0..expect.len() {
                assert_eq!(expect[i], output[i]);
            }
            fill_random(&mut output);
            for i in 0..expect.len() {
                expect[i] = input[i] ^ output[i];
            }
            slice_xor(&input, &mut output);
            for i in 0..expect.len() {
                assert_eq!(expect[i], output[i]);
            }
        }
    }

    #[test]
    fn test_div_a_is_0() {
        assert_eq!(0, div(0, 100));
    }

    #[test]
    #[should_panic]
    fn test_div_b_is_0() {
        div(1, 0);
    }

    #[test]
    fn test_same_as_maybe_ffi() {
        let len = 10_003;
        for _ in 0..100 {
            let c = rand::random::<u8>();
            let mut input = vec![0; len];
            fill_random(&mut input);
            {
                let mut output = vec![0; len];
                fill_random(&mut output);
                let mut output_copy = output.clone();

                mul_slice(c, &input, &mut output);
                mul_slice(c, &input, &mut output_copy);

                assert_eq!(output, output_copy);
            }
            {
                let mut output = vec![0; len];
                fill_random(&mut output);
                let mut output_copy = output.clone();

                mul_slice_xor(c, &input, &mut output);
                mul_slice_xor(c, &input, &mut output_copy);

                assert_eq!(output, output_copy);
            }
        }
    }
}
