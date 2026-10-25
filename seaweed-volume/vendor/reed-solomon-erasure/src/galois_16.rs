//! GF(2^16) implementation.
//!
//! More accurately, this is a `GF((2^8)^2)` implementation which builds an extension
//! field of `GF(2^8)`, as defined in the `galois_8` module.

use crate::galois_8;
use core::ops::{Add, Div, Mul, Sub};

// the irreducible polynomial used as a modulus for the field.
// print R.irreducible_element(2,algorithm="first_lexicographic" )
// x^2 + a*x + a^7
//
// hopefully it is a fast polynomial
const EXT_POLY: [u8; 3] = [1, 2, 128];

/// The field GF(2^16).
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq)]
pub struct Field;

impl crate::Field for Field {
    const ORDER: usize = 65536;

    type Elem = [u8; 2];

    fn add(a: [u8; 2], b: [u8; 2]) -> [u8; 2] {
        (Element(a) + Element(b)).0
    }

    fn mul(a: [u8; 2], b: [u8; 2]) -> [u8; 2] {
        (Element(a) * Element(b)).0
    }

    fn div(a: [u8; 2], b: [u8; 2]) -> [u8; 2] {
        (Element(a) / Element(b)).0
    }

    fn exp(elem: [u8; 2], n: usize) -> [u8; 2] {
        Element(elem).exp(n).0
    }

    fn zero() -> [u8; 2] {
        [0; 2]
    }

    fn one() -> [u8; 2] {
        [0, 1]
    }

    fn nth_internal(n: usize) -> [u8; 2] {
        [(n >> 8) as u8, n as u8]
    }
}

/// Type alias of ReedSolomon over GF(2^8).
pub type ReedSolomon = crate::ReedSolomon<Field>;

/// Type alias of ShardByShard over GF(2^8).
pub type ShardByShard<'a> = crate::ShardByShard<'a, Field>;

/// An element of `GF(2^16)`.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct Element(pub [u8; 2]);

impl Element {
    // Create the zero element.
    fn zero() -> Self {
        Element([0, 0])
    }

    // A constant element evaluating to `n`.
    fn constant(n: u8) -> Element {
        Element([0, n])
    }

    // Whether this is the zero element.
    fn is_zero(&self) -> bool {
        self.0 == [0; 2]
    }

    fn exp(mut self, n: usize) -> Element {
        if n == 0 {
            Element::constant(1)
        } else if self == Element::zero() {
            Element::zero()
        } else {
            let x = self;
            for _ in 1..n {
                self = self * x;
            }

            self
        }
    }

    // reduces from some polynomial with degree <= 2.
    #[inline]
    fn reduce_from(mut x: [u8; 3]) -> Self {
        if x[0] != 0 {
            // divide x by EXT_POLY and use remainder.
            // i = 0 here.
            // c*x^(i+j)  = a*x^i*b*x^j
            x[1] ^= galois_8::mul(EXT_POLY[1], x[0]);
            x[2] ^= galois_8::mul(EXT_POLY[2], x[0]);
        }

        Element([x[1], x[2]])
    }

    fn degree(&self) -> usize {
        if self.0[0] != 0 {
            1
        } else {
            0
        }
    }
}

impl From<[u8; 2]> for Element {
    fn from(c: [u8; 2]) -> Self {
        Element(c)
    }
}

impl Default for Element {
    fn default() -> Self {
        Element::zero()
    }
}

impl Add for Element {
    type Output = Element;

    fn add(self, other: Self) -> Element {
        Element([self.0[0] ^ other.0[0], self.0[1] ^ other.0[1]])
    }
}

impl Sub for Element {
    type Output = Element;

    fn sub(self, other: Self) -> Element {
        self.add(other)
    }
}

impl Mul for Element {
    type Output = Element;

    fn mul(self, rhs: Self) -> Element {
        // FOIL; our elements are linear at most, with two coefficients
        let out: [u8; 3] = [
            galois_8::mul(self.0[0], rhs.0[0]),
            galois_8::add(
                galois_8::mul(self.0[1], rhs.0[0]),
                galois_8::mul(self.0[0], rhs.0[1]),
            ),
            galois_8::mul(self.0[1], rhs.0[1]),
        ];

        Element::reduce_from(out)
    }
}

impl Mul<u8> for Element {
    type Output = Element;

    fn mul(self, rhs: u8) -> Element {
        Element([galois_8::mul(rhs, self.0[0]), galois_8::mul(rhs, self.0[1])])
    }
}

impl Div for Element {
    type Output = Element;

    fn div(self, rhs: Self) -> Element {
        self * rhs.inverse()
    }
}

// helpers for division.

#[derive(Debug)]
enum EgcdRhs {
    Element(Element),
    ExtPoly,
}

impl Element {
    // compute extended euclidean algorithm against an element of self,
    // where the GCD is known to be constant.
    fn const_egcd(self, rhs: EgcdRhs) -> (u8, Element, Element) {
        if self.is_zero() {
            let rhs = match rhs {
                EgcdRhs::Element(elem) => elem,
                EgcdRhs::ExtPoly => panic!("const_egcd invoked with divisible"),
            };
            (rhs.0[1], Element::constant(0), Element::constant(1))
        } else {
            let (cur_quotient, cur_remainder) = match rhs {
                EgcdRhs::Element(rhs) => rhs.polynom_div(self),
                EgcdRhs::ExtPoly => Element::div_ext_by(self),
            };

            // GCD is constant because EXT_POLY is irreducible
            let (g, x, y) = cur_remainder.const_egcd(EgcdRhs::Element(self));
            (g, y + (cur_quotient * x), x)
        }
    }

    // divide EXT_POLY by self.
    fn div_ext_by(rhs: Self) -> (Element, Element) {
        if rhs.degree() == 0 {
            // dividing by constant is the same as multiplying by another constant.
            // and all constant multiples of EXT_POLY are in the equivalence class
            // of 0.
            return (Element::zero(), Element::zero());
        }

        // divisor is ensured linear here.
        // now ensure divisor is monic.
        let leading_mul_inv = galois_8::div(1, rhs.0[0]);

        let monictized = rhs * leading_mul_inv;
        let mut poly = EXT_POLY;

        for i in 0..2 {
            let coef = poly[i];
            for j in 1..2 {
                if rhs.0[j] != 0 {
                    poly[i + j] ^= galois_8::mul(monictized.0[j], coef);
                }
            }
        }

        let remainder = Element::constant(poly[2]);
        let quotient = Element([poly[0], poly[1]]) * leading_mul_inv;

        (quotient, remainder)
    }

    fn polynom_div(self, rhs: Self) -> (Element, Element) {
        let divisor_degree = rhs.degree();
        if rhs.is_zero() {
            panic!("divide by 0");
        } else if self.degree() < divisor_degree {
            // If divisor's degree (len-1) is bigger, all dividend is a remainder
            (Element::zero(), self)
        } else if divisor_degree == 0 {
            // divide by constant.
            let invert = galois_8::div(1, rhs.0[1]);
            let quotient = Element([
                galois_8::mul(invert, self.0[0]),
                galois_8::mul(invert, self.0[1]),
            ]);

            (quotient, Element::zero())
        } else {
            // self degree is at least divisor degree, divisor degree not 0.
            // therefore both are 1.
            debug_assert_eq!(self.degree(), divisor_degree);
            debug_assert_eq!(self.degree(), 1);

            // ensure rhs is constant.
            let leading_mul_inv = galois_8::div(1, rhs.0[0]);
            let monic = Element([
                galois_8::mul(leading_mul_inv, rhs.0[0]),
                galois_8::mul(leading_mul_inv, rhs.0[1]),
            ]);

            let leading_coeff = self.0[0];
            let mut remainder = self.0[1];

            if monic.0[1] != 0 {
                remainder ^= galois_8::mul(monic.0[1], self.0[0]);
            }

            (
                Element::constant(galois_8::mul(leading_mul_inv, leading_coeff)),
                Element::constant(remainder),
            )
        }
    }

    /// Convert the inverse of this field element. Panics if zero.
    fn inverse(self) -> Element {
        if self.is_zero() {
            panic!("Cannot invert 0");
        }

        // first step of extended euclidean algorithm.
        // done here because EXT_POLY is outside the scope of `Element`.
        let (gcd, y) = {
            // self / EXT_POLY = (0, self)
            let remainder = self;

            // GCD is constant because EXT_POLY is irreducible
            let (g, x, _) = remainder.const_egcd(EgcdRhs::ExtPoly);

            (g, x)
        };

        // we still need to normalize it by dividing by the gcd
        if gcd != 0 {
            // EXT_POLY is irreducible so the GCD will always be constant.
            // EXT_POLY*x + self*y = gcd
            // self*y = gcd - EXT_POLY*x
            //
            // EXT_POLY*x is representative of the equivalence class of 0.
            let normalizer = galois_8::div(1, gcd);
            y * normalizer
        } else {
            // self is equivalent to zero.
            panic!("Cannot invert 0");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::Arbitrary;

    impl Arbitrary for Element {
        fn arbitrary<G: quickcheck::Gen>(gen: &mut G) -> Self {
            let a = u8::arbitrary(gen);
            let b = u8::arbitrary(gen);

            Element([a, b])
        }
    }

    quickcheck! {
        fn qc_add_associativity(a: Element, b: Element, c: Element) -> bool {
            a + (b + c) == (a + b) + c
        }

        fn qc_mul_associativity(a: Element, b: Element, c: Element) -> bool {
            a * (b * c) == (a * b) * c
        }

        fn qc_additive_identity(a: Element) -> bool {
            let zero = Element::zero();
            a - (zero - a) == zero
        }

        fn qc_multiplicative_identity(a: Element) -> bool {
            a.is_zero() || {
                let one = Element([0, 1]);
                (one / a) * a == one
            }
        }

        fn qc_add_commutativity(a: Element, b: Element) -> bool {
            a + b == b + a
        }

        fn qc_mul_commutativity(a: Element, b: Element) -> bool {
            a * b == b * a
        }

        fn qc_add_distributivity(a: Element, b: Element, c: Element) -> bool {
            a * (b + c) == (a * b) + (a * c)
        }

        fn qc_inverse(a: Element) -> bool {
            a.is_zero() || {
                let inv = a.inverse();
                a * inv == Element::constant(1)
            }
        }

        fn qc_exponent_1(a: Element, n: u8) -> bool {
            a.is_zero() || n == 0 || {
                let mut b = a.exp(n as usize);
                for _ in 1..n {
                    b = b / a;
                }

                a == b
            }
        }

        fn qc_exponent_2(a: Element, n: u8) -> bool {
            a.is_zero() || {
                let mut res = true;
                let mut b = Element::constant(1);

                for i in 0..n {
                    res = res && b == a.exp(i as usize);
                    b = b * a;
                }

                res
            }
        }

        fn qc_exp_zero_is_one(a: Element) -> bool {
            a.exp(0) == Element::constant(1)
        }
    }

    #[test]
    #[should_panic]
    fn test_div_b_is_0() {
        let _ = Element([1, 0]) / Element::zero();
    }

    #[test]
    fn zero_to_zero_is_one() {
        assert_eq!(Element::zero().exp(0), Element::constant(1))
    }
}
