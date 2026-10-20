//! Order independence, sliding-window retraction, merge and serialization.
//! Every comparison is on bits (NaN compared as NaN).

use m0_exact_acc::*;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

/// Values mixing every regime: normal-magnitude, full-range, subnormal,
/// signed zeros, exact negations (cancellation), duplicates, and, when
/// `nonfinite`, occasional NaN / ±inf.
fn mixed_values(rng: &mut StdRng, n: usize, nonfinite: bool) -> Vec<f64> {
    let mut out: Vec<f64> = Vec::with_capacity(n);
    while out.len() < n {
        let r: f64 = rng.gen();
        let v = if nonfinite && r < 0.02 {
            [f64::NAN, f64::INFINITY, f64::NEG_INFINITY][rng.gen_range(0..3)]
        } else if r < 0.3 {
            rng.gen_range(-1e4..1e4)
        } else if r < 0.5 {
            // any finite double
            f64::from_bits(
                (rng.gen::<u64>() % 0x7FF0_0000_0000_0000) | (rng.gen::<u64>() & 1 << 63),
            )
        } else if r < 0.6 {
            f64::from_bits(rng.gen::<u64>() & ((1 << 52) - 1)) * if rng.gen() { 1.0 } else { -1.0 }
        } else if r < 0.65 {
            [0.0, -0.0, f64::MAX, -f64::MAX, 5e-324, 1e308, -1e308][rng.gen_range(0..7)]
        } else if r < 0.85 && !out.is_empty() {
            let p = out[rng.gen_range(0..out.len())];
            if p.is_finite() {
                -p
            } else {
                1.0
            }
        } else {
            (rng.gen_range(0..1_000_000) as f64) / 100.0
        };
        out.push(v);
    }
    out
}

fn bits(x: Option<f64>) -> Option<u64> {
    x.map(|v| {
        if v.is_nan() {
            f64::NAN.to_bits()
        } else {
            v.to_bits()
        }
    })
}

/// Fingerprint of every finalizer over (x, y) pairs fed through `accs`.
#[derive(Clone)]
struct Five {
    sx: ExactAcc,
    sy: ExactAcc,
    sxx: ExactAcc,
    syy: ExactAcc,
    sxy: ExactAcc,
    n: u64,
}

impl Five {
    fn new() -> Self {
        Five {
            sx: ExactAcc::new(),
            sy: ExactAcc::new(),
            sxx: ExactAcc::new(),
            syy: ExactAcc::new(),
            sxy: ExactAcc::new(),
            n: 0,
        }
    }
    fn add(&mut self, x: f64, y: f64) {
        self.sx.add(x);
        self.sy.add(y);
        self.sxx.add_product(x, x);
        self.syy.add_product(y, y);
        self.sxy.add_product(x, y);
        self.n += 1;
    }
    fn sub(&mut self, x: f64, y: f64) {
        self.sx.sub(x);
        self.sy.sub(y);
        self.sxx.sub_product(x, x);
        self.syy.sub_product(y, y);
        self.sxy.sub_product(x, y);
        self.n -= 1;
    }
    fn merge(&mut self, o: &Five) {
        self.sx.merge(&o.sx);
        self.sy.merge(&o.sy);
        self.sxx.merge(&o.sxx);
        self.syy.merge(&o.syy);
        self.sxy.merge(&o.sxy);
        self.n += o.n;
    }
    fn roundtrip(&self) -> Five {
        let rt = |a: &ExactAcc| {
            let mut buf = Vec::new();
            a.serialize(&mut buf);
            ExactAcc::deserialize(&buf)
        };
        Five {
            sx: rt(&self.sx),
            sy: rt(&self.sy),
            sxx: rt(&self.sxx),
            syy: rt(&self.syy),
            sxy: rt(&self.sxy),
            n: self.n,
        }
    }
    fn fingerprint(&self) -> Vec<Option<u64>> {
        let n = self.n;
        vec![
            bits(Some(self.sx.round())),
            bits(Some(self.sy.round())),
            bits(Some(self.sxx.round())),
            bits(Some(self.syy.round())),
            bits(Some(self.sxy.round())),
            bits(exact_mean(&self.sx, n)),
            bits(exact_var(&self.sx, &self.sxx, n)),
            bits(exact_std(&self.sx, &self.sxx, n)),
            bits(exact_var(&self.sy, &self.syy, n)),
            bits(exact_cov(&self.sxy, &self.sx, &self.sy, n)),
            bits(exact_corr(
                &self.sxy, &self.sx, &self.sy, &self.sxx, &self.syy, n,
            )),
        ]
    }
}

fn feed(pairs: &[(f64, f64)]) -> Five {
    let mut f = Five::new();
    for &(x, y) in pairs {
        f.add(x, y);
    }
    f
}

#[test]
fn order_and_partition_independence() {
    let mut rng = StdRng::seed_from_u64(7);
    for (round, &n) in [2usize, 5, 17, 100, 1000, 5000]
        .iter()
        .cycle()
        .take(36)
        .enumerate()
    {
        let nonfinite = round % 6 == 5;
        let xs = mixed_values(&mut rng, n, nonfinite);
        let ys = mixed_values(&mut rng, n, nonfinite);
        let mut pairs: Vec<(f64, f64)> = xs.into_iter().zip(ys).collect();
        let want = feed(&pairs).fingerprint();
        for trial in 0..6 {
            pairs.shuffle(&mut rng);
            assert_eq!(
                feed(&pairs).fingerprint(),
                want,
                "shuffle n={n} trial={trial}"
            );
            // random cut points, partial states merged in random order,
            // half of them shipped through bytes
            let k = rng.gen_range(1..=8.min(n));
            let mut cuts: Vec<usize> = (0..k - 1).map(|_| rng.gen_range(0..=n)).collect();
            cuts.push(0);
            cuts.push(n);
            cuts.sort_unstable();
            let mut parts: Vec<Five> = cuts.windows(2).map(|w| feed(&pairs[w[0]..w[1]])).collect();
            parts.shuffle(&mut rng);
            let mut total = Five::new();
            for (i, p) in parts.iter().enumerate() {
                if i % 2 == 0 {
                    total.merge(&p.roundtrip());
                } else {
                    total.merge(p);
                }
            }
            assert_eq!(total.fingerprint(), want, "merge n={n} trial={trial} k={k}");
            // tree-shaped merge: pairwise
            let mut level = parts;
            while level.len() > 1 {
                let mut next = Vec::new();
                for ch in level.chunks(2) {
                    let mut a = ch[0].clone();
                    if let Some(b) = ch.get(1) {
                        a.merge(b);
                    }
                    next.push(a.roundtrip());
                }
                level = next;
            }
            assert_eq!(
                level[0].fingerprint(),
                want,
                "tree merge n={n} trial={trial}"
            );
        }
    }
}

#[test]
fn sliding_windows_match_fresh_frames() {
    let mut rng = StdRng::seed_from_u64(11);
    let n = 1500;
    let xs = mixed_values(&mut rng, n, true);
    let ys = mixed_values(&mut rng, n, true);
    for w in [1usize, 3, 50] {
        let mut slide = Five::new();
        for i in 0..n {
            slide.add(xs[i], ys[i]);
            if i >= w {
                slide.sub(xs[i - w], ys[i - w]);
            }
            let lo = (i + 1).saturating_sub(w);
            let mut fresh = Five::new();
            for j in lo..=i {
                fresh.add(xs[j], ys[j]);
            }
            assert_eq!(slide.fingerprint(), fresh.fingerprint(), "w={w} row={i}");
        }
        // after retracting everything the state is an exact zero again
        for i in n.saturating_sub(w)..n {
            slide.sub(xs[i], ys[i]);
        }
        assert_eq!(slide.sx.round().to_bits(), 0);
        assert_eq!(slide.sxy.round().to_bits(), 0);
        assert!(!slide.sx.has_nonfinite() && !slide.sxy.has_nonfinite());
    }
}

#[test]
fn sliding_window_long_run_with_retraction_of_extremes() {
    // A long stream where huge values enter and leave: after they leave, the
    // window result must not remember them.
    let mut rng = StdRng::seed_from_u64(3);
    let w = 64;
    let n = 20_000;
    let xs: Vec<f64> = (0..n)
        .map(|i| {
            if i % 997 == 0 {
                1e308
            } else if i % 991 == 0 {
                -f64::MAX
            } else if i % 1009 == 0 {
                f64::INFINITY
            } else {
                rng.gen_range(-100.0..100.0)
            }
        })
        .collect();
    let (mut s1, mut s2) = (ExactAcc::new(), ExactAcc::new());
    for i in 0..n {
        s1.add(xs[i]);
        s2.add_product(xs[i], xs[i]);
        if i >= w {
            s1.sub(xs[i - w]);
            s2.sub_product(xs[i - w], xs[i - w]);
        }
        if i % 101 == 0 {
            let (mut f1, mut f2) = (ExactAcc::new(), ExactAcc::new());
            for &x in &xs[(i + 1).saturating_sub(w)..=i] {
                f1.add(x);
                f2.add_product(x, x);
            }
            let m = ((i + 1).min(w)) as u64;
            assert_eq!(bits(Some(s1.round())), bits(Some(f1.round())), "row {i}");
            assert_eq!(
                bits(exact_var(&s1, &s2, m)),
                bits(exact_var(&f1, &f2, m)),
                "row {i}"
            );
            assert_eq!(
                bits(exact_std(&s1, &s2, m)),
                bits(exact_std(&f1, &f2, m)),
                "row {i}"
            );
        }
    }
}

/// Feeds pairs through the slice APIs in random-length batches.
fn feed_slices(rng: &mut StdRng, pairs: &[(f64, f64)]) -> Five {
    let xs: Vec<f64> = pairs.iter().map(|p| p.0).collect();
    let ys: Vec<f64> = pairs.iter().map(|p| p.1).collect();
    let mut f = Five::new();
    let mut i = 0;
    while i < xs.len() {
        let len = [1, 7, 600, 3000][rng.gen_range(0..4)].min(xs.len() - i);
        let (bx, by) = (&xs[i..i + len], &ys[i..i + len]);
        f.sx.add_slice(bx);
        f.sy.add_slice(by);
        f.sxx.add_squares(bx);
        f.syy.add_products(by, by);
        f.sxy.add_products(bx, by);
        f.n += len as u64;
        i += len;
    }
    f
}

#[test]
fn slice_paths_match_scalar_paths() {
    let mut rng = StdRng::seed_from_u64(21);
    for (round, &n) in [1usize, 600, 5000, 20_000]
        .iter()
        .cycle()
        .take(16)
        .enumerate()
    {
        let nonfinite = round % 3 == 2;
        let xs = mixed_values(&mut rng, n, nonfinite);
        let ys = mixed_values(&mut rng, n, nonfinite);
        let pairs: Vec<(f64, f64)> = xs.into_iter().zip(ys).collect();
        let want = feed(&pairs).fingerprint();
        assert_eq!(feed_slices(&mut rng, &pairs).fingerprint(), want, "n={n}");
    }
}

#[test]
fn sliding_windows_with_batch_retraction() {
    // retract_batch-style: the frame advances by a batch of rows at a time
    let mut rng = StdRng::seed_from_u64(23);
    let n = 30_000;
    let xs = mixed_values(&mut rng, n, true);
    let ys = mixed_values(&mut rng, n, true);
    for (w, step) in [(1000usize, 700usize), (2500, 1300), (600, 600)] {
        let mut slide = Five::new();
        let mut lo = 0;
        let mut hi = 0;
        while hi < n {
            let new_hi = (hi + step).min(n);
            let (bx, by) = (&xs[hi..new_hi], &ys[hi..new_hi]);
            slide.sx.add_slice(bx);
            slide.sy.add_slice(by);
            slide.sxx.add_squares(bx);
            slide.syy.add_squares(by);
            slide.sxy.add_products(bx, by);
            slide.n += (new_hi - hi) as u64;
            hi = new_hi;
            let new_lo = hi.saturating_sub(w);
            let (rx, ry) = (&xs[lo..new_lo], &ys[lo..new_lo]);
            slide.sx.sub_slice(rx);
            slide.sy.sub_slice(ry);
            slide.sxx.sub_squares(rx);
            slide.syy.sub_squares(ry);
            slide.sxy.sub_products(rx, ry);
            slide.n -= (new_lo - lo) as u64;
            lo = new_lo;
            let fresh = feed(
                &xs[lo..hi]
                    .iter()
                    .copied()
                    .zip(ys[lo..hi].iter().copied())
                    .collect::<Vec<_>>(),
            );
            assert_eq!(
                slide.fingerprint(),
                fresh.fingerprint(),
                "w={w} step={step} frame={lo}..{hi}"
            );
        }
    }
}

#[test]
fn serialize_roundtrip_and_sizes() {
    let mut rng = StdRng::seed_from_u64(5);
    for n in [0usize, 1, 2, 10, 1000] {
        for nonfinite in [false, true] {
            let xs = mixed_values(&mut rng, n, nonfinite);
            let mut a = ExactAcc::new();
            let mut p = ExactAcc::new();
            for &x in &xs {
                a.add(x);
                p.add_product(x, x * 0.5);
            }
            for acc in [&a, &p] {
                let mut buf = Vec::new();
                acc.serialize(&mut buf);
                let b = ExactAcc::deserialize(&buf);
                assert_eq!(bits(Some(b.round())), bits(Some(acc.round())));
                // re-serializing the decoded state gives the same bytes
                let mut buf2 = Vec::new();
                b.serialize(&mut buf2);
                assert_eq!(buf, buf2);
                // and the decoded state keeps accumulating exactly
                let mut c = b.clone();
                let mut d = acc.clone();
                for &x in &xs {
                    c.add(x);
                    d.add(x);
                }
                assert_eq!(bits(Some(c.round())), bits(Some(d.round())));
            }
        }
    }
    // empty state: 3 bytes; malformed input is rejected
    let mut buf = Vec::new();
    ExactAcc::new().serialize(&mut buf);
    assert_eq!(buf.len(), 3);
    assert!(ExactAcc::try_deserialize(&[]).is_none());
    assert!(ExactAcc::try_deserialize(&[0, 5, 1, 0, 0]).is_none());
    assert!(ExactAcc::try_deserialize(&[4, 0, 0]).is_none());
    let mut one = ExactAcc::new();
    one.add(1.0);
    let mut buf = Vec::new();
    one.serialize(&mut buf);
    assert_eq!(buf.len(), 3 + 4);
}

#[test]
fn equal_states_after_add_sub_of_all_regimes() {
    // add then sub of the same values (in another order) is an exact zero,
    // and the non-finite counts return to zero.
    let mut rng = StdRng::seed_from_u64(9);
    let xs = mixed_values(&mut rng, 3000, true);
    let mut a = ExactAcc::new();
    let mut p = ExactAcc::new();
    for &x in &xs {
        a.add(x);
        p.add_product(x, 3.0 * x);
    }
    let mut order = xs.clone();
    order.shuffle(&mut rng);
    for &x in &order {
        a.sub(x);
        p.sub_product(x, 3.0 * x);
    }
    assert_eq!(a.round().to_bits(), 0);
    assert_eq!(p.round().to_bits(), 0);
    assert!(!a.has_nonfinite() && !p.has_nonfinite());
    let mut buf = Vec::new();
    a.serialize(&mut buf);
    assert_eq!(buf, vec![0, 0, 0]);
}

#[test]
fn products_are_exact_not_fma_or_rounded() {
    // (1 + 2^-52)^2 = 1 + 2^-51 + 2^-104: rounding the product first would
    // lose 2^-104, which the following subtraction exposes.
    let x = 1.0 + f64::EPSILON;
    let mut p = ExactAcc::new();
    p.add_product(x, x);
    p.sub(1.0);
    p.sub(2.0 * f64::EPSILON);
    assert_eq!(p.round(), 2f64.powi(-104));
    // smallest subnormal squared is 2^-2148, far below f64; still exact
    let mut q = ExactAcc::new();
    q.add_product(5e-324, 5e-324);
    q.add_product(5e-324, -5e-324);
    assert_eq!(q.round().to_bits(), 0);
    q.add_product(5e-324, 5e-324);
    assert_eq!(q.round().to_bits(), 0); // nonzero 2^-2148 underflows to +0.0
    q.add_product(f64::MAX, f64::MAX);
    q.sub_product(f64::MAX, f64::MAX);
    q.add_product(-1e-300, 1e-300);
    assert_eq!(q.round(), -0.0);
    assert!(q.round().is_sign_negative()); // tiny negative keeps its sign
                                           // 0 * inf is NaN, inf * -2 is -inf, and both retract
    let mut r = ExactAcc::new();
    r.add_product(0.0, f64::INFINITY);
    assert!(r.round().is_nan());
    r.sub_product(0.0, f64::INFINITY);
    r.add_product(f64::INFINITY, -2.0);
    assert_eq!(r.round(), f64::NEG_INFINITY);
    r.sub_product(f64::INFINITY, -2.0);
    assert_eq!(r.round().to_bits(), 0);
}

#[test]
fn memory_is_proportional_to_exponent_span() {
    let mut a = ExactAcc::new();
    assert_eq!(a.heap_bytes(), 0);
    a.add(1.0);
    assert_eq!(a.heap_bytes(), 2 * 8); // a value touches two chunks
    for i in 0..1000 {
        a.add(1.0 + i as f64 * 0.001);
    }
    assert!(a.heap_bytes() <= 4 * 8, "{}", a.heap_bytes());
    a.add(0.0); // zeros never grow the range
    a.add(-0.0);
    assert!(a.heap_bytes() <= 4 * 8);
    a.add(5e-324);
    a.add(f64::MAX);
    assert!(a.heap_bytes() <= 70 * 8, "{}", a.heap_bytes());
}
