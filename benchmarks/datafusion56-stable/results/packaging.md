# Fat-LTO release build on stable 56 (#296, PR #423)

Both wheels were built with each tree's own `[profile.release]` (fat LTO,
`codegen-units = 1`, `opt-level = 3`, `strip = "symbols"`; the same in both
`Cargo.toml`s), `maturin build --release --locked`, offline, rustc 1.97.1,
maturin 1.14.1, CPython 3.13, x86_64 Linux (glibc 2.41), on the machine of
`../README.md`. Neither build printed a warning. The RC1 archive's wheels
were thin-LTO with 16 codegen units; these are the shipped profile.

| | 55.0.0 (`291aa14`) | 56.0.0 stable (PR #423 at `f3c9404`) |
|---|---|---|
| Wall time (55 from a cold target directory; 56 in the checkout's existing `target/`, so not comparable) | 19 min 58 s | 15 min 14 s |
| Wheel | `py_ltseq-0.1.0-cp313-cp313-manylinux_2_34_x86_64.whl`, 30,726,006 bytes | same name, 33,473,280 bytes |
| Wheel SHA-256 | `f31871f00a21513d1f8a9c98f8e83485111c503ea62eefeae5d4e3a21df9d56e` | `9fd84bebe5c62fe2da5aea8bea967db9138344af1a8b06810d64a270b2a1aa74` |
| `ltseq_core` `.so` | 84,240,248 bytes | 91,333,672 bytes (+8.4 %) |
| `.so` SHA-256 | `436307a6c12b66fadb99ae61f8b93c11fc68a56fbb686a7a3370fdea1e62df02` | `a1c57aec22d835d921b94af9864593ed0f29315fc4998938e144c74c837e69bc` |

Each wheel, unpacked, imported and ran everything in `deep-distinct-*.json`,
`repro-*.json.gz`, `plans-55.json`, `plans-56.json`, `witnesses-*.json`, `bench.md` and
`memory.md`. The hashes identify these local builds only; wheels are not
reproducible byte for byte across machines, and none was published.

```bash
maturin build --release --locked -o /var/tmp/wheels56                 # PR #423's checkout
(cd /var/tmp/base && CARGO_TARGET_DIR=/var/tmp/base/target \
    maturin build --release --locked -o /var/tmp/wheels55)            # 291aa14, exported
```

Only Linux x86_64 was built here. macOS (Apple Silicon) comes from CI, per
the owner's decision; CI's jobs build debug, not this release profile.
