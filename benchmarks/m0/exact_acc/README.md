# m2's exact accumulator, tested on its own

`src/m0/exact_acc.rs` holds the superaccumulator behind m2's exact
statistics (#251). This crate builds that one file as a library so its
reference tests run without LTSeq:

```bash
cd benchmarks/m0/exact_acc
python gen_vectors.py > vectors.json   # ~1 min; exact Fraction references
RUSTUP_TOOLCHAIN=1.96.1 cargo test --release
# More than 2^31 worst-case adds, with overflow checks on (~20 s):
RUSTUP_TOOLCHAIN=1.96.1 cargo test --profile release-checked --test overflow -- --ignored
# Per-element and per-finalizer costs:
RUSTUP_TOOLCHAIN=1.96.1 cargo run --release --bin bench
```

`gen_vectors.py` computes 3259 cases with Python's `Fraction` and checks every
rounding against the exact midpoints; `tests/vectors.rs` builds each case five
ways (forward, reversed, batched, as three merged parts, and merged after a
serialize round trip) and compares bit for bit. `tests/properties.rs` checks
order and partition independence, exact window retraction and serialization.
