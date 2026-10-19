//! Exact accumulation of `f64` sums and `f64` products, with correctly rounded
//! finalizers for sum, mean, sample variance, sample standard deviation,
//! sample covariance and Pearson correlation.
//!
//! m2's accumulator (#251). Its reference tests, against an independent
//! `Fraction` implementation, live in `benchmarks/m0/exact_acc`, and use
//! parts of the API m2 does not.
//!
//! Every result is the exact mathematical value rounded once to the nearest
//! `f64` (ties to even). No intermediate value is ever rounded.
//!
//! # Representation
//!
//! A finite `f64` is `m * 2^e` with `m` a 53-bit integer and `e` in
//! `[-1074, 971]`; a product of two is `ma*mb * 2^(ea+eb)` with a 106-bit
//! integer and `ea+eb` in `[-2148, 1942]`. Both live in one fixed-point space:
//! binary weight `2^e` sits at bit position `p = e + OFFSET` (`OFFSET` = 2176
//! keeps `p >= 0` for every product), and bit positions are grouped into
//! chunks of 32: chunk `k` has weight `2^(32k - OFFSET)`.
//!
//! An accumulator stores only the chunk range it has touched (`base` plus a
//! boxed slice of `i64`), so a group whose values span a narrow exponent range
//! costs a few chunks. Each chunk holds a signed 64-bit integer although it
//! only "owns" 32 bits of weight; the spare 31 bits absorb carries, which are
//! propagated lazily (Neal 2015, "small superaccumulator").
//!
//! Adding a value splits `±m << (p mod 32)` (two's complement) into its low
//! 32 bits, added to chunk `k = p / 32`, and the signed rest (below 2^52 in
//! magnitude), added to chunk `k + 1`. A 106-bit product uses five 32-bit
//! pieces on five chunks. Non-finite values never touch the chunks: they are
//! counted (NaN, +inf, -inf) so `sub` can retract them.
//!
//! # Overflow invariant
//!
//! Every chunk satisfies `|c| < 2^32 * (units + 1)`. A normalized state
//! (`units == 0`) has all chunks but the top one in `[0, 2^32)` and the top
//! one in `[-2^31, 2^31)`. A value add changes a chunk by less than 2^52 and
//! costs `VALUE_UNITS = 2^20` units; a product add changes a chunk by less
//! than `2^31 + 2^32` and costs `PRODUCT_UNITS = 2`; `merge` adds the two
//! bounds and costs `other.units + 1`. The state is normalized once `units`
//! reaches `UNIT_LIMIT = 2^30` (every 1024 value adds), so
//! `|c| < 2^32 * (2^30 + 2^20 + 1) < 2^63` always holds.
//!
//! # Finalizers
//!
//! `round` (the sum) needs no allocation: it propagates carries in one pass
//! for the sign and a second pass that keeps the top three magnitude digits
//! plus a sticky bit. The statistics convert accumulators to
//! `BigInt * 2^exp`, do exact integer arithmetic, then round once through a
//! truncating division or integer square root that keeps at least 60
//! significant bits and a sticky bit for anything discarded, followed by the
//! shared [`round_to_f64`] helper.

#![allow(dead_code)]

use num_bigint::{BigInt, BigUint, Sign};
use num_traits::{ToPrimitive, Zero};

const FRAC_MASK: u64 = (1 << 52) - 1;
const EXP_MASK: u64 = 0x7FF;
const INF_BITS: u64 = 0x7FF0_0000_0000_0000;
const SIGN_BIT: u64 = 1 << 63;
/// Bit position of binary weight `2^e` is `e + OFFSET`. The smallest weight
/// of a product of two `f64` is `2^-2148`; 2176 = 68 * 32 keeps every
/// position non-negative.
const OFFSET: u32 = 2176;
const LOW32: i64 = 0xFFFF_FFFF;
/// Normalize once this many units of additions have accumulated (see the
/// module docs for the bound).
const UNIT_LIMIT: u32 = 1 << 30;
/// Unit cost of one value add (its high piece is below 2^52 = 2^20 * 2^32).
const VALUE_UNITS: u32 = 1 << 20;
/// Unit cost of one product add (no piece reaches 2 * 2^32).
const PRODUCT_UNITS: u32 = 2;

/// Counts of non-finite values taking part. Signed so that `sub` and `merge`
/// of partial states compose; a count is "taking part" when it is positive.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct NonFinite {
    nan: i64,
    pos_inf: i64,
    neg_inf: i64,
}

#[derive(Clone, Copy)]
enum Kind {
    Nan,
    PosInf,
    NegInf,
}

/// Exact accumulator for a sum of `f64` values and/or exact `f64` products.
///
/// 32 bytes inline; the heap holds 8 bytes per occupied 32-bit chunk (plus
/// 24 bytes once a non-finite value has been seen).
#[derive(Clone, Debug, Default)]
pub struct ExactAcc {
    /// Chunk `base + i` is `chunks[i]`. Empty means the exact value is zero.
    chunks: Box<[i64]>,
    /// Allocated on the first non-finite value only.
    nonfinite: Option<Box<NonFinite>>,
    base: i32,
    /// Additions since the last normalization (see the overflow invariant).
    units: u32,
}

/// Adds `sm * 2^(p - OFFSET)` (`|sm| < 2^53`) to the two chunks it touches:
/// `sm << s == hi * 2^32 + lo` with `lo` in `[0, 2^32)` and
/// `hi = sm >> (32 - s)` (floor), `|hi| < 2^52`. `false` (nothing changed)
/// if the chunks are outside `chunks`.
#[inline(always)]
fn put_signed(chunks: &mut [i64], base: i32, sm: i64, p: u32) -> bool {
    // Negative differences wrap to >= 2^31, which always fails the check.
    let idx = ((p >> 5) as i32).wrapping_sub(base) as u32 as usize;
    let Some(c) = chunks.get_mut(idx..idx + 2) else {
        return false;
    };
    let s = p & 31;
    c[0] += (sm << s) & LOW32;
    c[1] += sm >> (32 - s);
    true
}

/// `put_signed` for `±m`, `m < 2^53`.
#[inline(always)]
fn put_value(chunks: &mut [i64], base: i32, m: u64, p: u32, neg: bool) -> bool {
    let mask = -(neg as i64);
    put_signed(chunks, base, ((m as i64) ^ mask) - mask, p)
}

/// Adds `±ma * mb * 2^(p - OFFSET)` (`ma, mb < 2^53`) to the five chunks it
/// touches; `false` if they are outside `chunks`. Each chunk changes by less
/// than 2^33 in magnitude (counted as two units).
#[inline(always)]
fn put_product(chunks: &mut [i64], base: i32, ma: u64, mb: u64, p: u32, neg: bool) -> bool {
    let idx = ((p >> 5) as i32).wrapping_sub(base) as u32 as usize;
    let Some(c) = chunks.get_mut(idx..idx + 5) else {
        return false;
    };
    // The exact product has up to 106 bits; shifting it by up to 31 bits
    // would need 137, so its low 64 bits and its (signed) high part are
    // shifted separately.
    let mask = -(neg as i128);
    let sp = ((ma as u128 * mb as u128) as i128 ^ mask) - mask;
    let s = p & 31;
    let lo = ((sp as u64) as u128) << s; // < 2^95
    let hi = (sp >> 64) << s; // |hi| < 2^73
    c[0] += lo as i64 & LOW32;
    c[1] += (lo as u64 >> 32) as i64;
    c[2] += (lo >> 64) as i64 + (hi as i64 & LOW32);
    c[3] += (hi as u64 >> 32) as i64;
    c[4] += (hi >> 64) as i64;
    true
}

/// Fast path of `add`/`sub`: `bits` must be a normal double. The sign is
/// applied to the mantissa before shifting (`NEG` flips it, for `sub`).
#[inline(always)]
fn put_normal<const NEG: bool>(chunks: &mut [i64], base: i32, bits: u64) -> bool {
    let p = ((bits >> 52) & EXP_MASK) as u32 + (OFFSET - 1075);
    let m = ((bits & FRAC_MASK) | (1 << 52)) as i64;
    let mask = ((bits as i64) >> 63) ^ -(NEG as i64);
    put_signed(chunks, base, (m ^ mask) - mask, p)
}

/// Fast path of `add_product`/`sub_product`: both factors normal.
#[inline(always)]
fn put_normal_product<const NEG: bool>(chunks: &mut [i64], base: i32, a: u64, b: u64) -> bool {
    let (ma, pa) = split_normal(a);
    let (mb, pb) = split_normal(b);
    let neg = ((a ^ b) & SIGN_BIT != 0) ^ NEG;
    put_product(chunks, base, ma, mb, pa + pb - OFFSET, neg)
}

/// True for normal doubles (not zero, subnormal, infinite or NaN): the only
/// inputs the fast paths take.
#[inline(always)]
fn is_normal(bits: u64) -> bool {
    ((bits >> 52) & EXP_MASK).wrapping_sub(1) < EXP_MASK - 1
}

/// Mantissa and lsb position of a normal double (see `split`).
#[inline(always)]
fn split_normal(bits: u64) -> (u64, u32) {
    let bexp = ((bits >> 52) & EXP_MASK) as u32;
    ((bits & FRAC_MASK) | (1 << 52), bexp + (OFFSET - 1075))
}

/// Splits finite `f64` bits into the integer mantissa `m` and the bit
/// position `p` of its least significant bit (`x = ±m * 2^(p - OFFSET)`).
#[inline(always)]
fn split(bits: u64) -> (u64, u32) {
    let bexp = ((bits >> 52) & EXP_MASK) as u32;
    let m = (bits & FRAC_MASK) | (((bexp != 0) as u64) << 52);
    // lsb exponent is max(bexp, 1) - 1075 (subnormals share bexp 1's scale)
    let p = bexp.max(1) + (OFFSET - 1075);
    (m, p)
}

#[inline(always)]
fn is_nonfinite(bits: u64) -> bool {
    (bits >> 52) & EXP_MASK == EXP_MASK
}

fn kind_of_value(bits: u64) -> Kind {
    if bits & FRAC_MASK != 0 {
        Kind::Nan
    } else if bits & SIGN_BIT == 0 {
        Kind::PosInf
    } else {
        Kind::NegInf
    }
}

/// Class of `a * b` when at least one factor is non-finite: NaN if either is
/// NaN or it is 0 * inf, otherwise an infinity with the product's sign.
fn kind_of_product(a: u64, b: u64) -> Kind {
    let nan = |x: u64| is_nonfinite(x) && x & FRAC_MASK != 0;
    let zero = |x: u64| x << 1 == 0;
    if nan(a) || nan(b) || zero(a) || zero(b) {
        Kind::Nan
    } else if (a ^ b) & SIGN_BIT == 0 {
        Kind::PosInf
    } else {
        Kind::NegInf
    }
}

impl ExactAcc {
    /// An empty accumulator (exact value zero, no allocation).
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `x` exactly.
    #[inline]
    pub fn add(&mut self, x: f64) {
        self.add_value::<false>(x.to_bits());
    }

    /// Subtracts `x` exactly; exactly undoes `add(x)`, including the
    /// non-finite counts.
    #[inline]
    pub fn sub(&mut self, x: f64) {
        self.add_value::<true>(x.to_bits());
    }

    /// Adds the exact (never rounded) product `a * b`.
    #[inline]
    pub fn add_product(&mut self, a: f64, b: f64) {
        self.add_prod::<false>(a.to_bits(), b.to_bits());
    }

    /// Subtracts the exact product `a * b`; exactly undoes `add_product(a, b)`.
    #[inline]
    pub fn sub_product(&mut self, a: f64, b: f64) {
        self.add_prod::<true>(a.to_bits(), b.to_bits());
    }

    #[inline(always)]
    fn add_value<const NEG: bool>(&mut self, bits: u64) {
        if is_normal(bits) && put_normal::<NEG>(&mut self.chunks, self.base, bits) {
            self.units += VALUE_UNITS;
            if self.units >= UNIT_LIMIT {
                self.normalize();
            }
            return;
        }
        self.add_value_slow(bits, NEG);
    }

    /// Non-finite (counted), zero (ignored, so it never grows the range),
    /// subnormal, or out of the current chunk range (grow, then add).
    #[cold]
    #[inline(never)]
    fn add_value_slow(&mut self, bits: u64, negate: bool) {
        if is_nonfinite(bits) {
            self.count(kind_of_value(bits), negate);
            return;
        }
        let (m, p) = split(bits);
        if m == 0 {
            return;
        }
        let k = (p >> 5) as i32;
        self.cover(k, k + 2);
        let neg = (bits & SIGN_BIT != 0) ^ negate;
        let done = put_value(&mut self.chunks, self.base, m, p, neg);
        debug_assert!(done);
        self.units += VALUE_UNITS;
        if self.units >= UNIT_LIMIT {
            self.normalize();
        }
    }

    #[inline(always)]
    fn add_prod<const NEG: bool>(&mut self, a: u64, b: u64) {
        if is_normal(a) & is_normal(b)
            && put_normal_product::<NEG>(&mut self.chunks, self.base, a, b)
        {
            self.units += PRODUCT_UNITS;
            if self.units >= UNIT_LIMIT {
                self.normalize();
            }
            return;
        }
        self.add_prod_slow(a, b, NEG);
    }

    #[cold]
    #[inline(never)]
    fn add_prod_slow(&mut self, a: u64, b: u64, negate: bool) {
        if is_nonfinite(a) | is_nonfinite(b) {
            self.count(kind_of_product(a, b), negate);
            return;
        }
        let (ma, pa) = split(a);
        let (mb, pb) = split(b);
        if ma == 0 || mb == 0 {
            return;
        }
        let p = pa + pb - OFFSET;
        let k = (p >> 5) as i32;
        self.cover(k, k + 5);
        let neg = ((a ^ b) & SIGN_BIT != 0) ^ negate;
        let done = put_product(&mut self.chunks, self.base, ma, mb, p, neg);
        debug_assert!(done);
        self.units += PRODUCT_UNITS;
        if self.units >= UNIT_LIMIT {
            self.normalize();
        }
    }

    /// Adds every value of `xs`; same result as `add` on each, faster for
    /// long slices (an Arrow array in `update_batch`).
    pub fn add_slice(&mut self, xs: &[f64]) {
        self.slice_values::<false>(xs);
    }

    /// Subtracts every value of `xs` (`retract_batch`).
    pub fn sub_slice(&mut self, xs: &[f64]) {
        self.slice_values::<true>(xs);
    }

    /// Adds `a[i] * b[i]` exactly for every `i`. Panics if lengths differ.
    pub fn add_products(&mut self, a: &[f64], b: &[f64]) {
        self.slice_products::<false>(a, b);
    }

    /// Subtracts `a[i] * b[i]` exactly for every `i`. Panics if lengths differ.
    pub fn sub_products(&mut self, a: &[f64], b: &[f64]) {
        self.slice_products::<true>(a, b);
    }

    /// Adds `x * x` exactly for every `x` (the `Σx²` of a variance); same as
    /// `add_products(xs, xs)` but cheaper, since a square is never negative.
    pub fn add_squares(&mut self, xs: &[f64]) {
        self.slice_squares::<false>(xs);
    }

    /// Subtracts `x * x` exactly for every `x`.
    pub fn sub_squares(&mut self, xs: &[f64]) {
        self.slice_squares::<true>(xs);
    }

    // The slice paths run the fast path on a local view of the chunks, so the
    // base, length and chunk pointer stay in registers (a loop over `add`
    // reloads them after every store, which may alias `self`), and they
    // count units per run instead of per element. Anything the fast path
    // does not take goes through the same slow path as `add`.

    fn slice_values<const NEG: bool>(&mut self, xs: &[f64]) {
        let mut rest = xs;
        while !rest.is_empty() {
            // units < UNIT_LIMIT between operations, so the budget is >= 0
            let budget = ((UNIT_LIMIT - 1 - self.units) / VALUE_UNITS) as usize;
            let run = &rest[..rest.len().min(budget)];
            let base = self.base;
            let chunks = &mut *self.chunks;
            let mut done = run.len();
            for (j, &x) in run.iter().enumerate() {
                let bits = x.to_bits();
                if !(is_normal(bits) && put_normal::<NEG>(chunks, base, bits)) {
                    done = j;
                    break;
                }
            }
            self.units += done as u32 * VALUE_UNITS;
            if self.units > UNIT_LIMIT - 1 - VALUE_UNITS {
                self.normalize();
            }
            rest = &rest[done..];
            if done < run.len() {
                self.add_value_slow(rest[0].to_bits(), NEG);
                rest = &rest[1..];
            }
        }
    }

    fn slice_products<const NEG: bool>(&mut self, a: &[f64], b: &[f64]) {
        assert_eq!(a.len(), b.len(), "add_products: length mismatch");
        let (mut ra, mut rb) = (a, b);
        while !ra.is_empty() {
            let budget = ((UNIT_LIMIT - 1 - self.units) / PRODUCT_UNITS) as usize;
            let n = ra.len().min(budget);
            let base = self.base;
            let chunks = &mut *self.chunks;
            let mut done = n;
            for (j, (&x, &y)) in ra[..n].iter().zip(&rb[..n]).enumerate() {
                let (xb, yb) = (x.to_bits(), y.to_bits());
                if !(is_normal(xb) & is_normal(yb)
                    && put_normal_product::<NEG>(chunks, base, xb, yb))
                {
                    done = j;
                    break;
                }
            }
            self.units += PRODUCT_UNITS * done as u32;
            if self.units > UNIT_LIMIT - 1 - PRODUCT_UNITS {
                self.normalize();
            }
            ra = &ra[done..];
            rb = &rb[done..];
            if done < n {
                self.add_prod_slow(ra[0].to_bits(), rb[0].to_bits(), NEG);
                ra = &ra[1..];
                rb = &rb[1..];
            }
        }
    }

    fn slice_squares<const NEG: bool>(&mut self, xs: &[f64]) {
        let mut rest = xs;
        while !rest.is_empty() {
            let budget = ((UNIT_LIMIT - 1 - self.units) / PRODUCT_UNITS) as usize;
            let run = &rest[..rest.len().min(budget)];
            let base = self.base;
            let chunks = &mut *self.chunks;
            let mut done = run.len();
            for (j, &x) in run.iter().enumerate() {
                let xb = x.to_bits();
                if !(is_normal(xb) && put_normal_product::<NEG>(chunks, base, xb, xb)) {
                    done = j;
                    break;
                }
            }
            self.units += PRODUCT_UNITS * done as u32;
            if self.units > UNIT_LIMIT - 1 - PRODUCT_UNITS {
                self.normalize();
            }
            rest = &rest[done..];
            if done < run.len() {
                let xb = rest[0].to_bits();
                self.add_prod_slow(xb, xb, NEG);
                rest = &rest[1..];
            }
        }
    }

    #[cold]
    #[inline(never)]
    fn count(&mut self, kind: Kind, negate: bool) {
        let nf = self.nonfinite.get_or_insert_with(Default::default);
        let d = if negate { -1 } else { 1 };
        match kind {
            Kind::Nan => nf.nan += d,
            Kind::PosInf => nf.pos_inf += d,
            Kind::NegInf => nf.neg_inf += d,
        }
    }

    /// Grows the chunk range to include chunks `[lo, hi)`.
    #[cold]
    #[inline(never)]
    fn cover(&mut self, lo: i32, hi: i32) {
        let len = self.chunks.len() as i32;
        let (nlo, nhi) = if len == 0 {
            (lo, hi)
        } else {
            (lo.min(self.base), hi.max(self.base + len))
        };
        if len != 0 && nlo == self.base && nhi == self.base + len {
            return;
        }
        let mut v = vec![0i64; (nhi - nlo) as usize];
        if len != 0 {
            let off = (self.base - nlo) as usize;
            v[off..off + len as usize].copy_from_slice(&self.chunks);
        }
        self.chunks = v.into_boxed_slice();
        self.base = nlo;
    }

    /// Propagates carries: all chunks but the top into `[0, 2^32)`, the top
    /// into `[-2^31, 2^31)` (growing by one chunk if needed).
    fn normalize(&mut self) {
        self.units = 0;
        let n = self.chunks.len();
        if n == 0 {
            return;
        }
        let mut carry = 0i64;
        for c in self.chunks[..n - 1].iter_mut() {
            let v = *c + carry;
            *c = v & LOW32;
            carry = v >> 32;
        }
        let top = self.chunks[n - 1] + carry;
        if (-(1i64 << 31)..(1i64 << 31)).contains(&top) {
            self.chunks[n - 1] = top;
        } else {
            let end = self.base + n as i32;
            self.cover(self.base, end + 1);
            self.chunks[n - 1] = top & LOW32;
            self.chunks[n] = top >> 32; // |top| < 2^63, so this is in [-2^31, 2^31)
        }
    }

    /// Adds another partial state (parallel aggregation).
    pub fn merge(&mut self, other: &ExactAcc) {
        if let Some(o) = other.nonfinite.as_deref() {
            let nf = self.nonfinite.get_or_insert_with(Default::default);
            nf.nan += o.nan;
            nf.pos_inf += o.pos_inf;
            nf.neg_inf += o.neg_inf;
        }
        if other.chunks.is_empty() {
            return;
        }
        // other.units < UNIT_LIMIT always holds between operations.
        if self.units + other.units + 1 >= UNIT_LIMIT {
            self.normalize();
        }
        self.cover(other.base, other.base + other.chunks.len() as i32);
        let off = (other.base - self.base) as usize;
        for (d, s) in self.chunks[off..].iter_mut().zip(other.chunks.iter()) {
            *d += *s;
        }
        self.units += other.units + 1;
        if self.units >= UNIT_LIMIT {
            self.normalize();
        }
    }

    /// The sum rules for non-finite values: NaN if any NaN or both infinities
    /// take part, else the one infinity, else `None` (finite).
    fn nonfinite_sum(&self) -> Option<f64> {
        let nf = self.nonfinite.as_deref()?;
        if nf.nan > 0 || (nf.pos_inf > 0 && nf.neg_inf > 0) {
            Some(f64::NAN)
        } else if nf.pos_inf > 0 {
            Some(f64::INFINITY)
        } else if nf.neg_inf > 0 {
            Some(f64::NEG_INFINITY)
        } else {
            None
        }
    }

    /// True when any NaN or infinity takes part.
    pub fn has_nonfinite(&self) -> bool {
        self.nonfinite
            .as_deref()
            .is_some_and(|nf| nf.nan > 0 || nf.pos_inf > 0 || nf.neg_inf > 0)
    }

    /// Sign of the exact finite value: `None` for zero, `Some(true)` if negative.
    fn sign(&self) -> Option<bool> {
        let mut carry = 0i64;
        let mut any = 0i64;
        for &c in self.chunks.iter() {
            let v = c + carry;
            any |= v & LOW32;
            carry = v >> 32;
        }
        // value = carry * 2^(32 n) + (digits in [0, 2^(32 n)))
        if carry < 0 {
            Some(true)
        } else if (carry | any) != 0 {
            Some(false)
        } else {
            None
        }
    }

    /// Calls `f(i, d)` for the normalized magnitude digits `d` (weight chunk
    /// `base + i`), `i` in `0..=len`.
    #[inline(always)]
    fn magnitude_digits(&self, neg: bool, mut f: impl FnMut(usize, u32)) {
        let mut carry = 0i64;
        for (i, &c) in self.chunks.iter().enumerate() {
            let v = if neg { -c } else { c } + carry;
            f(i, (v & LOW32) as u32);
            carry = v >> 32;
        }
        debug_assert!((0..=LOW32).contains(&carry));
        f(self.chunks.len(), carry as u32);
    }

    /// The exact finite value as `(neg, mant, exp, sticky)`: the value is
    /// `±(mant + t) * 2^exp` with `t = 0` if `!sticky` and `0 < t < 1`
    /// otherwise. `mant >= 2^64` whenever `sticky`. `None` for exact zero.
    fn top_bits(&self) -> Option<(bool, u128, i64, bool)> {
        let neg = self.sign()?;
        let (mut r0, mut r1, mut r2) = (0u32, 0u32, 0u32);
        let mut below = false;
        let mut snap = (0usize, 0u32, 0u32, 0u32, false);
        self.magnitude_digits(neg, |i, d| {
            below |= r2 != 0;
            r2 = r1;
            r1 = r0;
            r0 = d;
            if d != 0 {
                snap = (i, r0, r1, r2, below);
            }
        });
        let (h, w0, w1, w2, sticky) = snap;
        let mant = (w0 as u128) << 64 | (w1 as u128) << 32 | w2 as u128;
        let exp = 32 * (self.base as i64 + h as i64 - 2) - OFFSET as i64;
        Some((neg, mant, exp, sticky))
    }

    /// The exact finite value as `m * 2^e` (non-finite counts ignored).
    fn to_scaled(&self) -> Scaled {
        let Some(neg) = self.sign() else {
            return Scaled::zero();
        };
        let mut digits = Vec::with_capacity(self.chunks.len() + 1);
        self.magnitude_digits(neg, |_, d| digits.push(d));
        let low = digits.iter().position(|&d| d != 0).unwrap_or(0);
        let mag = BigUint::from_slice(&digits[low..]);
        let sign = if neg { Sign::Minus } else { Sign::Plus };
        Scaled {
            m: BigInt::from_biguint(sign, mag),
            e: 32 * (self.base as i64 + low as i64) - OFFSET as i64,
        }
    }

    /// The sum: the exact value rounded once to the nearest `f64` (ties to
    /// even), `+0.0` for an exact zero, ±inf only when the exact value rounds
    /// beyond the `f64` range; NaN if any NaN or both infinities take part,
    /// else the infinity if one sign of infinity takes part.
    pub fn round(&self) -> f64 {
        if let Some(r) = self.nonfinite_sum() {
            return r;
        }
        match self.top_bits() {
            None => 0.0,
            Some((neg, mant, exp, sticky)) => round_to_f64(neg, mant, exp, sticky),
        }
    }

    /// Appends a compact encoding of the state:
    /// `flags: u8` (bit 0: non-finite counts follow, bit 1: negative),
    /// `[nan, +inf, -inf: i64 LE]` if bit 0, `base: u8`, `len: u8`,
    /// `len` magnitude digits as `u32` LE (no zero digit at either end).
    pub fn serialize(&self, out: &mut Vec<u8>) {
        let nf = self
            .nonfinite
            .as_deref()
            .filter(|nf| **nf != NonFinite::default());
        let neg = self.sign();
        let mut digits: Vec<u32> = Vec::new();
        let mut low = 0usize;
        if let Some(neg) = neg {
            digits.reserve(self.chunks.len() + 1);
            self.magnitude_digits(neg, |_, d| digits.push(d));
            low = digits.iter().position(|&d| d != 0).unwrap_or(0);
            let high = digits.iter().rposition(|&d| d != 0).map_or(0, |h| h + 1);
            digits.truncate(high);
        }
        let flags = nf.is_some() as u8 | ((neg == Some(true)) as u8) << 1;
        out.push(flags);
        if let Some(nf) = nf {
            out.extend_from_slice(&nf.nan.to_le_bytes());
            out.extend_from_slice(&nf.pos_inf.to_le_bytes());
            out.extend_from_slice(&nf.neg_inf.to_le_bytes());
        }
        let digits = &digits[low.min(digits.len())..];
        let base = if digits.is_empty() {
            0
        } else {
            self.base + low as i32
        };
        // Chunk indices stay below 256: the top bit of any reachable value is
        // below position 2048 + 64 + OFFSET = 4288, i.e. chunk 134.
        out.push(u8::try_from(base).expect("chunk index fits in u8"));
        out.push(u8::try_from(digits.len()).expect("chunk count fits in u8"));
        for d in digits {
            out.extend_from_slice(&d.to_le_bytes());
        }
    }

    /// Decodes a state written by [`serialize`](Self::serialize).
    ///
    /// # Panics
    /// On malformed input; see [`try_deserialize`](Self::try_deserialize).
    pub fn deserialize(bytes: &[u8]) -> Self {
        Self::try_deserialize(bytes).expect("malformed ExactAcc state")
    }

    /// Decodes a state written by [`serialize`](Self::serialize); `None` if
    /// the bytes are malformed (length mismatch or unknown flags).
    pub fn try_deserialize(bytes: &[u8]) -> Option<Self> {
        let (&flags, mut rest) = bytes.split_first()?;
        if flags & !3 != 0 {
            return None;
        }
        let mut acc = ExactAcc::new();
        if flags & 1 != 0 {
            let mut take = || -> Option<i64> {
                let (head, tail) = rest.split_at_checked(8)?;
                rest = tail;
                Some(i64::from_le_bytes(head.try_into().ok()?))
            };
            let nf = NonFinite {
                nan: take()?,
                pos_inf: take()?,
                neg_inf: take()?,
            };
            acc.nonfinite = Some(Box::new(nf));
        }
        let [base, len, ref digits @ ..] = *rest else {
            return None;
        };
        if digits.len() != 4 * len as usize {
            return None;
        }
        if len > 0 {
            let sign = if flags & 2 != 0 { -1 } else { 1 };
            acc.base = base as i32;
            let (words, _) = digits.as_chunks::<4>();
            acc.chunks = words
                .iter()
                .map(|&d| sign * u32::from_le_bytes(d) as i64)
                .collect();
        }
        // |chunk| < 2^32 satisfies the invariant with units == 0.
        Some(acc)
    }

    /// Heap memory owned by this accumulator, in bytes (add
    /// `size_of::<ExactAcc>()` for the total).
    pub fn heap_bytes(&self) -> usize {
        self.chunks.len() * std::mem::size_of::<i64>()
            + self
                .nonfinite
                .as_ref()
                .map_or(0, |_| std::mem::size_of::<NonFinite>())
    }
}

/// An exact dyadic rational `m * 2^e`.
struct Scaled {
    m: BigInt,
    e: i64,
}

impl Scaled {
    fn zero() -> Self {
        Scaled {
            m: BigInt::zero(),
            e: 0,
        }
    }

    fn mul(&self, o: &Scaled) -> Scaled {
        Scaled {
            m: &self.m * &o.m,
            e: self.e + o.e,
        }
    }

    fn sub(self, o: Scaled) -> Scaled {
        if o.m.is_zero() {
            return self;
        }
        if self.m.is_zero() {
            return Scaled { m: -o.m, e: o.e };
        }
        let e = self.e.min(o.e);
        let a = self.m << (self.e - e) as usize;
        let b = o.m << (o.e - e) as usize;
        Scaled { m: a - b, e }
    }

    /// `n * sxy - sx * sy`: n² times the co-moment (a multiple of the exact
    /// covariance numerator), exact.
    fn comoment(sxy: Scaled, sx: &Scaled, sy: &Scaled, n: u64) -> Scaled {
        let nsxy = Scaled {
            m: sxy.m * n,
            e: sxy.e,
        };
        nsxy.sub(sx.mul(sy))
    }
}

/// Rounds `(-1)^neg * (mant + t) * 2^exp` to the nearest `f64`, ties to even,
/// where `t = 0` when `!sticky` and `0 < t < 1` when `sticky`.
///
/// Requires `mant > 0`, and `mant >= 2^53` when `sticky`: then at least one
/// bit of `mant` is dropped, so the sticky tail lies wholly below the
/// rounding point and only breaks exact ties. Callers keep 60+ bits. Handles normal, subnormal, underflow to ±0 (the sign is
/// kept for a nonzero value) and overflow to ±inf.
pub(crate) fn round_to_f64(neg: bool, mant: u128, exp: i64, sticky: bool) -> f64 {
    debug_assert!(mant != 0);
    debug_assert!(!sticky || mant >= 1 << 53);
    let sign = if neg { SIGN_BIT } else { 0 };
    let bl = 128 - mant.leading_zeros() as i64;
    let top = exp + bl - 1; // exponent of the leading bit
    if top > 1023 {
        return f64::from_bits(sign | INF_BITS);
    }
    let lsb = (top - 52).max(-1074); // exponent of the result's last bit
    let r = lsb - exp; // number of low bits of mant to drop
    let q: u64 = if r <= 0 {
        debug_assert!(!sticky);
        (mant << (-r) as u32) as u64 // exact, < 2^53
    } else if r > bl {
        0 // value < 2^(lsb - 1): below half an ulp of zero
    } else {
        // 1 <= r <= bl <= 128
        let q0 = if r == 128 { 0 } else { mant >> r };
        let rem = if r == 128 {
            mant
        } else {
            mant & ((1u128 << r) - 1)
        };
        let half = 1u128 << (r - 1);
        let up = rem > half || (rem == half && (sticky || q0 & 1 == 1));
        q0 as u64 + up as u64
    };
    // q < 2^53, or q == 2^53 after rounding up; for lsb == -1074, q < 2^52 is
    // a subnormal. Adding q (with its hidden bit) to the shifted exponent
    // yields the right encoding in every case, including the carry into the
    // next binade and into infinity.
    let bits = (((lsb + 1074) as u64) << 52) + q;
    if bits >= INF_BITS {
        return f64::from_bits(sign | INF_BITS);
    }
    f64::from_bits(sign | bits)
}

/// `num * 2^t` for `t >= 0`; `floor(num / 2^-t)` and whether any dropped bit
/// was set for `t < 0`.
fn shift_with_sticky(num: &BigUint, t: i64) -> (BigUint, bool) {
    if t >= 0 {
        (num << t as usize, false)
    } else {
        let s = t.unsigned_abs();
        let sticky = num.trailing_zeros().is_some_and(|tz| tz < s);
        (num >> s as usize, sticky)
    }
}

/// Correctly rounded `(-1)^neg * num / den * 2^exp`, `num, den > 0`.
fn div_round(neg: bool, num: &BigUint, den: &BigUint, exp: i64) -> f64 {
    // a = num * 2^t has bits(den) + 65 bits, so q = floor(a / den) is in
    // [2^64, 2^66). Truncating num (t < 0) keeps q exact: for integers a, d
    // and 0 <= eps < 1, floor((a + eps) / d) == floor(a / d).
    let t = den.bits() as i64 + 65 - num.bits() as i64;
    let (a, mut sticky) = shift_with_sticky(num, t);
    let q = &a / den;
    sticky |= &q * den != a;
    let q = q.to_u128().expect("quotient below 2^66");
    round_to_f64(neg, q, exp - t, sticky)
}

/// `floor(sqrt(x))` for `x < 2^126`.
fn isqrt_u128(x: u128) -> u128 {
    if x == 0 {
        return 0;
    }
    let mut s = ((x as f64).sqrt() as u128).max(1);
    s = (s + x / s) >> 1; // one Newton step from a 2^-52-accurate guess
    while s * s > x {
        s -= 1;
    }
    while (s + 1) * (s + 1) <= x {
        s += 1;
    }
    s
}

/// Correctly rounded `sqrt(num / den * 2^exp)`, `num, den > 0`.
fn sqrt_round(num: &BigUint, den: &BigUint, exp: i64) -> f64 {
    // Choose t with exp - t even so that Y = num * 2^t / den lies in
    // (2^119, 2^122); X = floor(Y) fits u128 and s = isqrt(X) has >= 60 bits.
    // floor(sqrt(Y)) == isqrt(floor(Y)) for real Y >= 0, and sqrt(Y) == s
    // exactly iff Y is an integer and s * s == X.
    let mut t = 120 + den.bits() as i64 - num.bits() as i64;
    if (exp - t) & 1 != 0 {
        t += 1;
    }
    let (a, mut sticky) = shift_with_sticky(num, t);
    let q = &a / den;
    sticky |= &q * den != a;
    let x = q.to_u128().expect("scaled radicand below 2^122");
    let s = isqrt_u128(x);
    sticky |= s * s != x;
    round_to_f64(false, s, (exp - t) / 2, sticky)
}

fn den_n_n1(n: u64) -> BigUint {
    BigUint::from(n as u128 * (n as u128 - 1))
}

/// Mean: exact `Σx / n` rounded once. `None` when `n == 0`. Non-finite values
/// follow the sum rules.
pub fn exact_mean(s1: &ExactAcc, n: u64) -> Option<f64> {
    if n == 0 {
        return None;
    }
    if let Some(r) = s1.nonfinite_sum() {
        return Some(r);
    }
    let s = s1.to_scaled();
    if s.m.is_zero() {
        return Some(0.0);
    }
    let neg = s.m.sign() == Sign::Minus;
    Some(div_round(neg, s.m.magnitude(), &BigUint::from(n), s.e))
}

/// `n Σx² − (Σx)²` (zero clamped from inconsistent inputs), or `Err(NaN)`.
fn var_numerator(s1: &ExactAcc, s2: &ExactAcc, n: u64) -> Result<Scaled, f64> {
    if s1.has_nonfinite() || s2.has_nonfinite() {
        return Err(f64::NAN);
    }
    let x = s1.to_scaled();
    let d = Scaled::comoment(s2.to_scaled(), &x, &x, n);
    if d.m.sign() == Sign::Minus {
        // Impossible when s2 holds Σx·x of the values in s1.
        debug_assert!(false, "Σx² inconsistent with Σx");
        return Ok(Scaled::zero());
    }
    Ok(d)
}

/// Sample variance: exact `(nΣx² − (Σx)²) / (n(n − 1))` rounded once, never
/// negative. `s2` must hold `Σ x·x` (via `add_product(x, x)`) of the values in
/// `s1`. `None` for `n < 2`; NaN when any value taking part is non-finite.
pub fn exact_var(s1: &ExactAcc, s2: &ExactAcc, n: u64) -> Option<f64> {
    if n < 2 {
        return None;
    }
    Some(match var_numerator(s1, s2, n) {
        Err(nan) => nan,
        Ok(d) if d.m.is_zero() => 0.0,
        Ok(d) => div_round(false, d.m.magnitude(), &den_n_n1(n), d.e),
    })
}

/// Sample standard deviation: the correctly rounded square root of the exact
/// sample variance (not of the rounded variance). Same domain as
/// [`exact_var`].
pub fn exact_std(s1: &ExactAcc, s2: &ExactAcc, n: u64) -> Option<f64> {
    if n < 2 {
        return None;
    }
    Some(match var_numerator(s1, s2, n) {
        Err(nan) => nan,
        Ok(d) if d.m.is_zero() => 0.0,
        Ok(d) => sqrt_round(d.m.magnitude(), &den_n_n1(n), d.e),
    })
}

/// Sample covariance: exact `(nΣab − ΣaΣb) / (n(n − 1))` rounded once.
/// `sab` holds `Σ a·b`. `None` for `n < 2`; NaN when any value taking part is
/// non-finite.
pub fn exact_cov(sab: &ExactAcc, sa: &ExactAcc, sb: &ExactAcc, n: u64) -> Option<f64> {
    if n < 2 {
        return None;
    }
    if sab.has_nonfinite() || sa.has_nonfinite() || sb.has_nonfinite() {
        return Some(f64::NAN);
    }
    let a = sa.to_scaled();
    let b = sb.to_scaled();
    let c = Scaled::comoment(sab.to_scaled(), &a, &b, n);
    if c.m.is_zero() {
        return Some(0.0);
    }
    let neg = c.m.sign() == Sign::Minus;
    Some(div_round(neg, c.m.magnitude(), &den_n_n1(n), c.e))
}

/// Pearson correlation: exact `(nΣab − ΣaΣb) / sqrt((nΣa² − (Σa)²)(nΣb² − (Σb)²))`
/// rounded once, computed as `sign(N) * sqrt(N² / (Da Db))`; lies in
/// `[-1, 1]` and `corr(a, a) == 1.0`. NaN when either variance is zero or any
/// value taking part is non-finite; `None` for `n < 2`.
pub fn exact_corr(
    sab: &ExactAcc,
    sa: &ExactAcc,
    sb: &ExactAcc,
    saa: &ExactAcc,
    sbb: &ExactAcc,
    n: u64,
) -> Option<f64> {
    if n < 2 {
        return None;
    }
    if [sab, sa, sb, saa, sbb].iter().any(|s| s.has_nonfinite()) {
        return Some(f64::NAN);
    }
    let a = sa.to_scaled();
    let b = sb.to_scaled();
    let da = Scaled::comoment(saa.to_scaled(), &a, &a, n);
    let db = Scaled::comoment(sbb.to_scaled(), &b, &b, n);
    if da.m.sign() != Sign::Plus || db.m.sign() != Sign::Plus {
        return Some(f64::NAN); // zero variance (negative only if inconsistent)
    }
    let num = Scaled::comoment(sab.to_scaled(), &a, &b, n);
    if num.m.is_zero() {
        return Some(0.0);
    }
    let nm = num.m.magnitude();
    let r = sqrt_round(
        &(nm * nm),
        &(da.m.magnitude() * db.m.magnitude()),
        2 * num.e - da.e - db.e,
    );
    Some(if num.m.sign() == Sign::Minus { -r } else { r })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_helper_ties_and_ranges() {
        // 1 + 2^-53: tie between 1 and 1 + 2^-52 -> even (1.0)
        let m = (1u128 << 53) + 1; // value m * 2^-53
        assert_eq!(round_to_f64(false, m, -53, false), 1.0);
        // with sticky the tie breaks upward
        assert_eq!(round_to_f64(false, m << 10, -63, true), 1.0 + f64::EPSILON);
        // 1 + 3 * 2^-53: tie between 1 + 2^-52 and 1 + 2^-51 -> even (upper)
        assert_eq!(
            round_to_f64(false, (1u128 << 53) + 3, -53, false),
            1.0 + 2.0 * f64::EPSILON
        );
        // smallest subnormal and half of it (tie -> 0), 3/4 of it (-> min)
        assert_eq!(round_to_f64(false, 1, -1074, false).to_bits(), 1);
        assert_eq!(round_to_f64(false, 1, -1075, false).to_bits(), 0);
        assert_eq!(round_to_f64(true, 1, -1075, false).to_bits(), SIGN_BIT);
        assert_eq!(round_to_f64(false, 3, -1076, false).to_bits(), 1);
        // largest subnormal rounding up into the smallest normal
        let big = ((1u128 << 52) - 1) * 2 + 1; // (2^52 - 0.5) * 2^-1074
        assert_eq!(round_to_f64(false, big, -1075, false), f64::MIN_POSITIVE);
        // overflow threshold: MAX + half ulp is a tie -> inf
        let mx = (1u128 << 53) - 1; // MAX = mx * 2^971
        assert_eq!(round_to_f64(false, 2 * mx + 1, 970, false), f64::INFINITY);
        assert_eq!(round_to_f64(false, mx, 971, false), f64::MAX);
        assert_eq!(
            round_to_f64(false, (2 * mx + 1) << 60, 910, true),
            f64::INFINITY
        );
        assert_eq!(round_to_f64(true, 1, 1024, false), f64::NEG_INFINITY);
        // far below the subnormal range
        assert_eq!(round_to_f64(false, u128::MAX, -1300, true).to_bits(), 0);
    }

    #[test]
    fn isqrt_exhaustive_small_and_edges() {
        for x in 0u128..20_000 {
            let s = isqrt_u128(x);
            assert!(s * s <= x && (s + 1) * (s + 1) > x, "{x}");
        }
        for k in [60u32, 61, 62] {
            let s = (1u128 << k) - 1;
            assert_eq!(isqrt_u128(s * s), s);
            assert_eq!(isqrt_u128(s * s - 1), s - 1);
            assert_eq!(isqrt_u128((s + 1) * (s + 1) - 1), s);
        }
    }

    #[test]
    fn struct_is_small() {
        assert_eq!(std::mem::size_of::<ExactAcc>(), 32);
    }

    // ---- output verifier for the rational rounding helpers -----------------
    //
    // Independent of the algorithms above: it does not recompute the result,
    // it checks that the returned double r satisfies mid_lo <= v <= mid_hi
    // (ties to the even neighbour) by exact integer comparisons.

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
        fn big(&mut self, bits: u64) -> BigUint {
            let mut digits = vec![0u32; bits.div_ceil(32) as usize];
            for d in digits.iter_mut() {
                *d = self.next() as u32;
            }
            let x = BigUint::from_slice(&digits) >> (digits.len() as u64 * 32 - bits) as usize;
            x | (BigUint::from(1u8) << (bits - 1) as usize) // exactly `bits` bits
        }
    }

    /// Exact dyadic `m * 2^e` of a finite non-negative double.
    fn dyadic(x: f64) -> (BigUint, i64) {
        let (m, p) = split(x.to_bits());
        (BigUint::from(m), p as i64 - OFFSET as i64)
    }

    /// (a + b) / 2 for dyadics, as a dyadic.
    fn midpoint(a: (BigUint, i64), b: (BigUint, i64)) -> (BigUint, i64) {
        let e = a.1.min(b.1);
        (
            (a.0 << (a.1 - e) as usize) + (b.0 << (b.1 - e) as usize),
            e - 1,
        )
    }

    /// Compares num * 2^exp / den with m * 2^me (squared when `square`).
    fn cmp_q(
        num: &BigUint,
        den: &BigUint,
        exp: i64,
        mid: &(BigUint, i64),
        square: bool,
    ) -> std::cmp::Ordering {
        let (m, me) = if square {
            (&mid.0 * &mid.0, 2 * mid.1)
        } else {
            (mid.0.clone(), mid.1)
        };
        // num * 2^exp  vs  m * den * 2^me
        let rhs = m * den;
        let e = exp.min(me);
        (num << (exp - e) as usize).cmp(&(rhs << (me - e) as usize))
    }

    /// Asserts r is the correctly rounded value (or square root, for
    /// `square`) of v = num * 2^exp / den > 0.
    fn verify(r: f64, num: &BigUint, den: &BigUint, exp: i64, square: bool) {
        use std::cmp::Ordering::*;
        assert!(r >= 0.0, "negative result");
        let ctx = || format!("r={r:e} num={num} den={den} exp={exp} sqrt={square}");
        let max_up = (BigUint::from(1u8), 1024i64); // 2^1024, the "next" after MAX
        if r.is_infinite() {
            let mid = midpoint(dyadic(f64::MAX), max_up);
            assert!(cmp_q(num, den, exp, &mid, square) != Less, "{}", ctx());
            return;
        }
        let even = r.to_bits() & 1 == 0;
        let up = if r == f64::MAX {
            max_up
        } else {
            dyadic(r.next_up())
        };
        let hi = midpoint(dyadic(r), up);
        match cmp_q(num, den, exp, &hi, square) {
            Greater => panic!("too low: {}", ctx()),
            Equal => assert!(even, "tie not even (hi): {}", ctx()),
            Less => {}
        }
        if r > 0.0 {
            let lo = midpoint(dyadic(r), dyadic(r.next_down()));
            match cmp_q(num, den, exp, &lo, square) {
                Less => panic!("too high: {}", ctx()),
                Equal => assert!(even, "tie not even (lo): {}", ctx()),
                Greater => {}
            }
        }
    }

    fn random_double(rng: &mut Rng) -> f64 {
        loop {
            let x = f64::from_bits(rng.next() >> 1);
            if x.is_finite() && x > 0.0 {
                return x;
            }
        }
    }

    #[test]
    fn div_round_random_rationals() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        for _ in 0..20_000 {
            let num = {
                let b = 1 + rng.below(400);
                rng.big(b)
            };
            let den = {
                let b = 1 + rng.below(300);
                rng.big(b)
            };
            let exp = rng.below(2600) as i64 - 1300;
            let r = div_round(false, &num, &den, exp);
            verify(r, &num, &den, exp, false);
            let s = sqrt_round(&num, &den, exp);
            verify(s, &num, &den, exp, true);
            let neg = div_round(true, &num, &den, exp);
            assert_eq!(neg.to_bits(), r.to_bits() ^ SIGN_BIT);
        }
    }

    /// Rationals within a hair of a rounding midpoint (above, below, or
    /// exactly on it), with the hair far below any guard bit: this is where
    /// a lost sticky bit or a short quotient shows up.
    #[test]
    fn div_and_sqrt_round_near_midpoints() {
        let mut rng = Rng(0xD1B5_4A32_D192_ED03);
        let mut ties = 0;
        for i in 0..20_000 {
            let f = match i % 4 {
                0 => f64::from_bits(1 + rng.below(1 << 53)), // subnormal / small normal
                1 => f64::MAX.next_down().next_down(),
                _ => random_double(&mut rng),
            };
            let (m, me) = midpoint(dyadic(f), dyadic(f.next_up()));
            let den = {
                let b = 1 + rng.below(200);
                rng.big(b)
            };
            let k = rng.below(300) as i64;
            let delta = BigUint::from(rng.below(3)); // 0: exact tie
            let above = rng.below(2) == 0;
            if delta.is_zero() {
                ties += 1;
            }
            let shift = |x: BigUint| x << k as usize;
            // division: v = num / den * 2^(me - k) = m * 2^me ± delta * 2^(me-k) / den
            let base = shift(&m * &den);
            let num = if above {
                &base + &delta
            } else {
                &base - &delta
            };
            let r = div_round(false, &num, &den, me - k);
            verify(r, &num, &den, me - k, false);
            // square root: v = m² * 2^(2me) ± delta * 2^(2me-k) / den
            let base = shift(&m * &m * &den);
            let num = if above {
                &base + &delta
            } else {
                &base - &delta
            };
            let s = sqrt_round(&num, &den, 2 * me - k);
            verify(s, &num, &den, 2 * me - k, true);
        }
        assert!(ties > 1000);
    }

    #[test]
    fn round_via_accumulator_near_midpoints() {
        // round(): the exact sum lands within one 2^-1074 of a midpoint.
        let mut rng = Rng(42);
        for _ in 0..20_000 {
            let f = random_double(&mut rng);
            let up = f.next_up();
            if !up.is_finite() {
                continue;
            }
            let half = (up - f) / 2.0; // exact: a power of two (or subnormal step)
            let mut acc = ExactAcc::new();
            acc.add(f);
            if half != 0.0 && half.is_finite() {
                acc.add(half);
            }
            let eps = [0.0, 5e-324, -5e-324][rng.below(3) as usize];
            acc.add(eps);
            let s = acc.to_scaled();
            verify(
                acc.round(),
                s.m.magnitude(),
                &BigUint::from(1u8),
                s.e,
                false,
            );
        }
    }
}
