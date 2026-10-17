//! Throughput benchmark: `cargo run --release --bin bench`.
//!
//! Data: 10,000,000 values like round(uniform(0, 10000), 2), i.e. k / 100 for
//! a random integer k < 10^6. Each measurement runs several times; the
//! minimum and median are reported.

use m0_exact_acc::*;
use std::hint::black_box;
use std::time::Instant;

struct SplitMix64(u64);
impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
}

/// Runs `f` `reps` times; returns (min, median) seconds.
fn time<R>(reps: usize, mut f: impl FnMut() -> R) -> (f64, f64) {
    let mut t: Vec<f64> = (0..reps)
        .map(|_| {
            let s = Instant::now();
            black_box(f());
            s.elapsed().as_secs_f64()
        })
        .collect();
    t.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (t[0], t[reps / 2])
}

fn report(name: &str, per: f64, (min, med): (f64, f64), unit: &str) {
    println!(
        "{name:<44} {:>9.3} {unit} (median {:>9.3})",
        min / per * 1e9,
        med / per * 1e9
    );
}

fn main() {
    const N: usize = 10_000_000;
    let mut rng = SplitMix64(42);
    let xs: Vec<f64> = (0..N)
        .map(|_| (rng.next() % 1_000_000) as f64 / 100.0)
        .collect();
    let xs = black_box(xs);
    let reps = 7;
    println!("{N} values, round(uniform(0, 10000), 2); best of {reps} (median)\n");
    println!("--- per element ---");

    let t = time(reps, || xs.iter().sum::<f64>());
    report("plain f64 iter().sum()", N as f64, t, "ns/elem");

    let t = time(reps, || {
        let mut a = ExactAcc::new();
        for &x in xs.iter() {
            a.add(x);
        }
        a
    });
    report("ExactAcc::add", N as f64, t, "ns/elem");

    let t = time(reps, || {
        let mut a = ExactAcc::new();
        for &x in xs.iter() {
            a.add_product(x, x);
        }
        a
    });
    report("ExactAcc::add_product(x, x)", N as f64, t, "ns/elem");

    let t = time(reps, || {
        let (mut s1, mut s2) = (ExactAcc::new(), ExactAcc::new());
        for &x in xs.iter() {
            s1.add(x);
            s2.add_product(x, x);
        }
        (s1, s2)
    });
    report("variance state: add + add_product", N as f64, t, "ns/elem");

    let t = time(reps, || {
        let mut a = ExactAcc::new();
        for b in xs.chunks(8192) {
            a.add_slice(b);
        }
        a
    });
    report(
        "ExactAcc::add_slice, 8192-value batches",
        N as f64,
        t,
        "ns/elem",
    );

    let t = time(reps, || {
        let mut a = ExactAcc::new();
        for b in xs.chunks(8192) {
            a.add_squares(b);
        }
        a
    });
    report(
        "ExactAcc::add_squares, 8192 batches",
        N as f64,
        t,
        "ns/elem",
    );

    let t = time(reps, || {
        let (mut s1, mut s2) = (ExactAcc::new(), ExactAcc::new());
        for b in xs.chunks(8192) {
            s1.add_slice(b);
            s2.add_squares(b);
        }
        (s1, s2)
    });
    report(
        "variance state: add_slice + add_squares",
        N as f64,
        t,
        "ns/elem",
    );

    let ys: Vec<f64> = (0..N).map(|i| xs[(i * 7 + 3) % N]).collect();
    let t = time(reps, || {
        let mut a = ExactAcc::new();
        for (&x, &y) in xs.iter().zip(&ys) {
            a.add_product(x, y);
        }
        a
    });
    report(
        "ExactAcc::add_product(x, y), distinct data",
        N as f64,
        t,
        "ns/elem",
    );

    let t = time(reps, || {
        let mut a = ExactAcc::new();
        for (bx, by) in xs.chunks(8192).zip(ys.chunks(8192)) {
            a.add_products(bx, by);
        }
        a
    });
    report(
        "ExactAcc::add_products(x, y), 8192 batches",
        N as f64,
        t,
        "ns/elem",
    );

    for w in [3usize, 1000] {
        let t = time(reps, || {
            let mut a = ExactAcc::new();
            for i in 0..N {
                a.add(xs[i]);
                if i >= w {
                    a.sub(xs[i - w]);
                }
            }
            a
        });
        report(
            &format!("sliding add+sub, window {w}"),
            N as f64,
            t,
            "ns/row",
        );
    }
    let w = 1000;
    let t = time(3, || {
        let mut a = ExactAcc::new();
        let mut out = 0u64;
        for i in 0..N {
            a.add(xs[i]);
            if i >= w {
                a.sub(xs[i - w]);
            }
            out ^= a.round().to_bits();
        }
        out
    });
    report(
        "sliding add+sub+round per row, window 1000",
        N as f64,
        t,
        "ns/row",
    );

    // finalizers on the full 10M-value state
    let (mut s1, mut s2) = (ExactAcc::new(), ExactAcc::new());
    for &x in xs.iter() {
        s1.add(x);
        s2.add_product(x, x);
    }
    let mut sb = ExactAcc::new();
    let mut sab = ExactAcc::new();
    let mut sbb = ExactAcc::new();
    for (i, &x) in xs.iter().enumerate() {
        let y = xs[(i * 7 + 3) % N];
        sb.add(y);
        sab.add_product(x, y);
        sbb.add_product(y, y);
    }
    let n = N as u64;
    println!("\n--- finalizers, per call (state of all 10M values) ---");
    const K: usize = 200_000;
    let t = time(reps, || {
        let mut acc = 0u64;
        for _ in 0..K {
            acc ^= black_box(&s1).round().to_bits();
        }
        acc
    });
    report("round (sum)", K as f64, t, "ns/call");
    let t = time(reps, || {
        let mut acc = 0u64;
        for _ in 0..K {
            acc ^= exact_mean(black_box(&s1), n).unwrap().to_bits();
        }
        acc
    });
    report("exact_mean", K as f64, t, "ns/call");
    let t = time(reps, || {
        let mut acc = 0u64;
        for _ in 0..K {
            acc ^= exact_var(black_box(&s1), &s2, n).unwrap().to_bits();
        }
        acc
    });
    report("exact_var", K as f64, t, "ns/call");
    let t = time(reps, || {
        let mut acc = 0u64;
        for _ in 0..K {
            acc ^= exact_std(black_box(&s1), &s2, n).unwrap().to_bits();
        }
        acc
    });
    report("exact_std", K as f64, t, "ns/call");
    let t = time(reps, || {
        let mut acc = 0u64;
        for _ in 0..K {
            acc ^= exact_cov(black_box(&sab), &s1, &sb, n).unwrap().to_bits();
        }
        acc
    });
    report("exact_cov", K as f64, t, "ns/call");
    let t = time(reps, || {
        let mut acc = 0u64;
        for _ in 0..K {
            acc ^= exact_corr(black_box(&sab), &s1, &sb, &s2, &sbb, n)
                .unwrap()
                .to_bits();
        }
        acc
    });
    report("exact_corr", K as f64, t, "ns/call");
    let t = time(reps, || {
        let mut v = Vec::with_capacity(64);
        let mut acc = 0usize;
        for _ in 0..K {
            v.clear();
            black_box(&s2).serialize(&mut v);
            acc += ExactAcc::deserialize(&v).heap_bytes();
        }
        acc
    });
    report(
        "serialize + deserialize (Σx² state)",
        K as f64,
        t,
        "ns/call",
    );

    println!("\n--- memory per accumulator (10M values) ---");
    let sz = std::mem::size_of::<ExactAcc>();
    let mut buf = Vec::new();
    s1.serialize(&mut buf);
    let ser1 = buf.len();
    buf.clear();
    s2.serialize(&mut buf);
    println!(
        "Σx  : {sz} B struct + {} B heap = {} B; serialized {ser1} B",
        s1.heap_bytes(),
        sz + s1.heap_bytes()
    );
    println!(
        "Σx² : {sz} B struct + {} B heap = {} B; serialized {} B",
        s2.heap_bytes(),
        sz + s2.heap_bytes(),
        buf.len()
    );
    println!(
        "results: sum={:e} mean={:e} var={:e} std={:e}",
        s1.round(),
        exact_mean(&s1, n).unwrap(),
        exact_var(&s1, &s2, n).unwrap(),
        exact_std(&s1, &s2, n).unwrap()
    );

    // grouped: 100,000 groups, random group per row (cache-missing access)
    const G: usize = 100_000;
    let gid: Vec<u32> = (0..N).map(|_| (rng.next() % G as u64) as u32).collect();
    println!("\n--- grouped: {G} groups, random group per row ---");
    let t = time(5, || {
        let mut s = vec![0f64; G];
        for (i, &x) in xs.iter().enumerate() {
            s[gid[i] as usize] += x;
        }
        s
    });
    report("plain f64 grouped sum", N as f64, t, "ns/elem");
    let t = time(5, || {
        let mut s: Vec<ExactAcc> = (0..G).map(|_| ExactAcc::new()).collect();
        for (i, &x) in xs.iter().enumerate() {
            s[gid[i] as usize].add(x);
        }
        s
    });
    report(
        "ExactAcc grouped add (incl. allocating)",
        N as f64,
        t,
        "ns/elem",
    );
    let mut g1: Vec<ExactAcc> = (0..G).map(|_| ExactAcc::new()).collect();
    let mut g2: Vec<ExactAcc> = (0..G).map(|_| ExactAcc::new()).collect();
    let t = time(5, || {
        for a in g1.iter_mut().chain(g2.iter_mut()) {
            *a = ExactAcc::new();
        }
        for (i, &x) in xs.iter().enumerate() {
            let g = gid[i] as usize;
            g1[g].add(x);
            g2[g].add_product(x, x);
        }
    });
    report("ExactAcc grouped add + add_product", N as f64, t, "ns/elem");
    let t = time(5, || {
        g1.iter().map(|a| a.round().to_bits()).fold(0, |a, b| a ^ b)
    });
    report("finalize: round per group", G as f64, t, "ns/group");
    let t = time(5, || {
        g1.iter()
            .zip(&g2)
            .map(|(a, b)| exact_var(a, b, 100).unwrap().to_bits())
            .fold(0, |a, b| a ^ b)
    });
    report("finalize: exact_var per group", G as f64, t, "ns/group");
    let h1: usize = g1.iter().map(|a| a.heap_bytes()).sum();
    let h2: usize = g2.iter().map(|a| a.heap_bytes()).sum();
    println!(
        "memory per group (~100 values each): Σx {:.1} B, Σx² {:.1} B (struct {sz} B + avg heap)",
        sz as f64 + h1 as f64 / G as f64,
        sz as f64 + h2 as f64 / G as f64
    );
}
