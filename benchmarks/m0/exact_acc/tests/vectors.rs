//! Checks every finalizer against the exact reference vectors produced by
//! gen_vectors.py (Python, fractions.Fraction, midpoint-verified rounding).
//! Each case is also run reversed, split + merged, and through
//! serialize/deserialize.

use m0_exact_acc::*;
use serde_json::Value;

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

/// Mirrors generate() in gen_vectors.py.
fn gen_values(spec: &Value) -> Vec<f64> {
    let n = spec["n"].as_u64().unwrap() as usize;
    let kind = spec["gen"].as_str().unwrap();
    if kind == "repeat" {
        return vec![parse(&spec["value"]).unwrap(); n];
    }
    if kind == "spike" {
        let mut v = vec![0.0; n];
        v[0] = parse(&spec["value"]).unwrap();
        return v;
    }
    let mut rng = SplitMix64(spec["seed"].as_u64().unwrap());
    (0..n)
        .map(|_| match kind {
            "uniform2dp" => (rng.next() % 1_000_000) as f64 / 100.0,
            "normal_mag" => {
                let (u, v) = (rng.next(), rng.next());
                let bexp = 1023 - 20 + (u % 41);
                f64::from_bits((u >> 63) << 63 | bexp << 52 | (v & ((1 << 52) - 1)))
            }
            "full_range" => {
                let (u, v) = (rng.next(), rng.next());
                let bexp = u % 2047;
                f64::from_bits((u >> 63) << 63 | bexp << 52 | (v & ((1 << 52) - 1)))
            }
            k => panic!("unknown generator {k}"),
        })
        .collect()
}

fn parse(v: &Value) -> Option<f64> {
    v.as_str()
        .map(|s| f64::from_bits(u64::from_str_radix(s, 16).unwrap()))
}

fn load(v: &Value) -> Vec<f64> {
    if let Some(bits) = v.get("bits") {
        return bits
            .as_array()
            .unwrap()
            .iter()
            .map(|b| parse(b).unwrap())
            .collect();
    }
    let vals = gen_values(v);
    for (i, h) in v["head"].as_array().unwrap().iter().enumerate() {
        assert_eq!(
            vals[i].to_bits(),
            parse(h).unwrap().to_bits(),
            "generator mismatch at value {i} of {v}"
        );
    }
    vals
}

fn same(a: Option<f64>, b: Option<f64>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => (x.is_nan() && y.is_nan()) || x.to_bits() == y.to_bits(),
        _ => false,
    }
}

/// All accumulators of one case, built in a chosen way.
struct Accs {
    sx: ExactAcc,
    sxx: ExactAcc,
    sy: ExactAcc,
    syy: ExactAcc,
    sxy: ExactAcc,
}

fn build(xs: &[f64], ys: Option<&[f64]>, order: &[usize]) -> Accs {
    let mut a = Accs {
        sx: ExactAcc::new(),
        sxx: ExactAcc::new(),
        sy: ExactAcc::new(),
        syy: ExactAcc::new(),
        sxy: ExactAcc::new(),
    };
    for &i in order {
        a.sx.add(xs[i]);
        a.sxx.add_product(xs[i], xs[i]);
        if let Some(ys) = ys {
            a.sy.add(ys[i]);
            a.syy.add_product(ys[i], ys[i]);
            a.sxy.add_product(xs[i], ys[i]);
        }
    }
    a
}

/// The slice paths (`add_slice`, `add_squares`, `add_products`), with inputs padded
/// by zeros past the scalar-fallback threshold so the lane kernel runs even
/// for tiny cases (zeros change no sum and 0 * 0 no product).
fn build_slices(xs: &[f64], ys: Option<&[f64]>) -> Accs {
    let pad = |v: &[f64]| {
        let mut p = v.to_vec();
        p.resize(v.len() + 700, 0.0);
        p
    };
    let px = pad(xs);
    let mut a = Accs {
        sx: ExactAcc::new(),
        sxx: ExactAcc::new(),
        sy: ExactAcc::new(),
        syy: ExactAcc::new(),
        sxy: ExactAcc::new(),
    };
    a.sx.add_slice(&px);
    a.sxx.add_squares(&px);
    if let Some(ys) = ys {
        let py = pad(ys);
        a.sy.add_slice(&py);
        a.syy.add_products(&py, &py); // the general product path for Σy²
        a.sxy.add_products(&px, &py);
    }
    a
}

fn merge_accs(parts: Vec<Accs>, via_bytes: bool) -> Accs {
    let mut it = parts.into_iter();
    let mut out = it.next().unwrap();
    let rt = |a: &ExactAcc| {
        if via_bytes {
            let mut buf = Vec::new();
            a.serialize(&mut buf);
            ExactAcc::deserialize(&buf)
        } else {
            a.clone()
        }
    };
    for p in it {
        out.sx.merge(&rt(&p.sx));
        out.sxx.merge(&rt(&p.sxx));
        out.sy.merge(&rt(&p.sy));
        out.syy.merge(&rt(&p.syy));
        out.sxy.merge(&rt(&p.sxy));
    }
    out
}

fn check(case: &Value, a: &Accs, how: &str, failures: &mut Vec<String>) {
    let name = case["name"].as_str().unwrap();
    let n = case["n"].as_u64().unwrap();
    let mut cmp = |what: &str, got: Option<f64>| {
        let want = parse(&case[what]);
        if !same(got, want) {
            failures.push(format!(
                "{name} [{how}] {what}: got {got:?} ({:016x?}), want {want:?} ({:016x?})",
                got.map(f64::to_bits),
                want.map(f64::to_bits)
            ));
        }
    };
    cmp("sum", Some(a.sx.round()));
    cmp("mean", exact_mean(&a.sx, n));
    cmp("sum_xx", Some(a.sxx.round()));
    cmp("var", exact_var(&a.sx, &a.sxx, n));
    cmp("std", exact_std(&a.sx, &a.sxx, n));
    if case.get("y").is_some() {
        cmp("sum_y", Some(a.sy.round()));
        cmp("sum_yy", Some(a.syy.round()));
        cmp("sum_xy", Some(a.sxy.round()));
        cmp("cov", exact_cov(&a.sxy, &a.sx, &a.sy, n));
        cmp("corr", exact_corr(&a.sxy, &a.sx, &a.sy, &a.sxx, &a.syy, n));
    }
}

#[test]
fn reference_vectors() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/vectors.json");
    let text = std::fs::read_to_string(path).expect("run gen_vectors.py > vectors.json");
    let doc: Value = serde_json::from_str(&text).unwrap();
    let cases = doc["cases"].as_array().unwrap();
    let mut failures = Vec::new();
    let mut checked = 0usize;
    for case in cases {
        let xs = load(&case["x"]);
        let ys = case.get("y").map(load);
        let ys = ys.as_deref();
        let n = xs.len();
        assert_eq!(n as u64, case["n"].as_u64().unwrap());
        let fwd: Vec<usize> = (0..n).collect();
        let rev: Vec<usize> = (0..n).rev().collect();
        check(case, &build(&xs, ys, &fwd), "forward", &mut failures);
        check(case, &build(&xs, ys, &rev), "reverse", &mut failures);
        check(case, &build_slices(&xs, ys), "slices", &mut failures);
        // three uneven parts, merged directly and through bytes
        let cuts = [0, n / 3, n / 3 + n / 5, n];
        let parts = |cuts: &[usize]| -> Vec<Accs> {
            cuts.windows(2)
                .rev()
                .map(|w| build(&xs, ys, &fwd[w[0]..w[1]]))
                .collect()
        };
        check(
            case,
            &merge_accs(parts(&cuts), false),
            "merged",
            &mut failures,
        );
        check(
            case,
            &merge_accs(parts(&cuts), true),
            "serialized",
            &mut failures,
        );
        checked += 1;
    }
    for f in failures.iter().take(40) {
        eprintln!("{f}");
    }
    assert!(
        failures.is_empty(),
        "{} mismatches over {checked} cases",
        failures.len()
    );
    eprintln!("checked {checked} reference cases x 5 build paths");
}

/// The contract's named examples, independent of the JSON file.
#[test]
fn contract_examples() {
    let acc = |xs: &[f64]| {
        let (mut s1, mut s2) = (ExactAcc::new(), ExactAcc::new());
        for &x in xs {
            s1.add(x);
            s2.add_product(x, x);
        }
        (s1, s2)
    };
    let (s1, s2) = acc(&[1e308, 1e308]);
    assert_eq!(exact_mean(&s1, 2), Some(1e308));
    assert_eq!(s1.round(), f64::INFINITY);
    let (s1, s2b) = acc(&[1e308, -1e308]);
    assert_eq!(exact_std(&s1, &s2b, 2), Some(1.4142135623730951e308));
    assert_eq!(exact_var(&s1, &s2b, 2), Some(f64::INFINITY));
    assert_eq!(s1.round().to_bits(), 0); // +0.0
    let (s1, _) = acc(&[-0.0, -0.0]);
    assert_eq!(s1.round().to_bits(), 0);
    let (s1, s2) = (s1, s2);
    assert_eq!(exact_var(&s1, &s2, 1), None);
    let (s1, _) = acc(&[0.1; 10]);
    assert_eq!(s1.round(), 1.0);
}
