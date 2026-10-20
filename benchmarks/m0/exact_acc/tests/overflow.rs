//! Worst-case lazy-carry stress: more than 2^31 additions whose chunk
//! increments are as large as the representation allows, so any slack in the
//! normalization bound overflows an i64 chunk. Slow (~15 s), so ignored by
//! default; run with overflow checks:
//!
//!     cargo test --profile release-checked --test overflow -- --ignored
//!
//! (With plain `--release` an overflow would wrap and show up as a mismatch.)

use m0_exact_acc::*;

/// A double whose mantissa is all ones and whose lsb sits at bit 31 of a
/// chunk: `add` puts 2^31 on its low chunk and 2^52 - 1 (the largest high
/// piece) on the next one.
fn worst_value() -> f64 {
    // bit position p = bexp + 1101; p mod 32 == 31  <=>  bexp mod 32 == 18
    let bexp = 1042u64;
    assert_eq!((bexp + 1101) % 32, 31);
    f64::from_bits(bexp << 52 | ((1 << 52) - 1))
}

/// A partner for `worst_value()` whose exact product (all-ones-ish 106-bit
/// mantissa product) has its lsb at bit 31 of a chunk.
fn worst_partner() -> f64 {
    for d in 0..32u64 {
        let pa = 1042 + 1101;
        let pb = 1023 + d + 1101;
        if (pa + pb - 2176) % 32 == 31 {
            return f64::from_bits((1023 + d) << 52 | ((1 << 52) - 1));
        }
    }
    unreachable!()
}

/// 2^k copies of `acc` by repeated self-merge (exercises the merge bound).
fn doubled(acc: &ExactAcc, k: u32) -> ExactAcc {
    let mut a = acc.clone();
    for _ in 0..k {
        let b = a.clone();
        a.merge(&b);
    }
    a
}

#[test]
#[ignore]
fn worst_case_sums_do_not_overflow() {
    let x = worst_value();
    let n: u64 = (1 << 31) + 12_345;
    let mut acc = ExactAcc::new();
    for _ in 0..n {
        acc.add(x);
    }
    // exact n * x, computed as one exact product
    let mut want = ExactAcc::new();
    want.add_product(x, n as f64);
    assert_eq!(acc.round().to_bits(), want.round().to_bits());
    assert_eq!(exact_mean(&acc, n), Some(x));
    // the slice path, in batches of mixed sizes
    let batch = vec![x; 1 << 20];
    let mut sl = ExactAcc::new();
    let mut left = n;
    while left > 0 {
        let len = left.min(batch.len() as u64 - (left % 7)) as usize;
        sl.add_slice(&batch[..len]);
        left -= len as u64;
    }
    assert_eq!(sl.round().to_bits(), want.round().to_bits());
    // retract all of it
    for _ in 0..n {
        acc.sub(x);
    }
    assert_eq!(acc.round().to_bits(), 0);
    // same via doubling merges: 2^31 copies
    let mut one = ExactAcc::new();
    one.add(-x);
    let d = doubled(&one, 31);
    let mut want = ExactAcc::new();
    want.add_product(-x, (1u64 << 31) as f64);
    assert_eq!(d.round().to_bits(), want.round().to_bits());
}

#[test]
#[ignore]
fn worst_case_products_do_not_overflow() {
    let (a, b) = (worst_value(), worst_partner());
    let n: u64 = (1 << 31) + 777;
    let mut acc = ExactAcc::new();
    for _ in 0..n {
        acc.add_product(a, b);
    }
    // n * a * b exactly: 2^31 copies by doubling, plus 777 more
    let mut one = ExactAcc::new();
    one.add_product(a, b);
    let mut want = doubled(&one, 31);
    for _ in 0..777 {
        want.add_product(a, b);
    }
    assert_eq!(acc.round().to_bits(), want.round().to_bits());
    // slice paths
    let (va, vb) = (vec![a; 1 << 20], vec![b; 1 << 20]);
    let mut sl = ExactAcc::new();
    for _ in 0..(n >> 20) {
        sl.add_products(&va, &vb);
    }
    sl.add_products(
        &va[..(n & ((1 << 20) - 1)) as usize],
        &vb[..(n & ((1 << 20) - 1)) as usize],
    );
    assert_eq!(sl.round().to_bits(), want.round().to_bits());
    let mut sq = ExactAcc::new();
    let mut sq_want = ExactAcc::new();
    for _ in 0..(1 << 11) {
        sq.add_squares(&va);
    }
    let mut one = ExactAcc::new();
    one.add_product(a, a);
    sq_want.merge(&doubled(&one, 31));
    assert_eq!(sq.round().to_bits(), sq_want.round().to_bits());
    // merge two full accumulators
    let mut m = acc.clone();
    m.merge(&acc);
    let mut w = want.clone();
    w.merge(&want);
    assert_eq!(m.round().to_bits(), w.round().to_bits());
}
