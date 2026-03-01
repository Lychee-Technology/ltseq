// WGSL Stream Compaction Shader for LTSeq GPU acceleration
//
// Compacts rows that pass a filter into a dense output array.
// Uses a two-dispatch approach:
//   Pass 1 (this shader, entry `prefix_sum`): compute per-word popcount prefix sum
//   Pass 2 (this shader, entry `scatter`):    scatter passing rows into output
//
// Binding layout:
//   @binding(0)  params          — uniform: num_rows, num_words
//   @binding(1)  mask            — storage read: bitmask (u32 packed, one bit per row)
//   @binding(2)  prefix_sums     — storage read_write: cumulative popcounts (u32 per word)
//   @binding(3)  input_col       — storage read: input column data (array<i32>)
//   @binding(4)  output_col      — storage read_write: compacted output column (array<i32>)
//
// The scatter entry only needs bindings 0-4.
// For i64 columns, input_col and output_col contain two i32 words per element,
// so the effective lengths are 2×num_rows. The `is_wide` flag in params controls this.

struct CompactParams {
    num_rows: u32,   // number of input rows
    num_words: u32,  // ceil(num_rows / 32) — number of mask words
    is_wide: u32,    // 1 = i64 column (two i32 words per element), 0 = narrow (one word)
    _pad: u32,
}

@group(0) @binding(0) var<uniform> params: CompactParams;
@group(0) @binding(1) var<storage, read> mask: array<u32>;
@group(0) @binding(2) var<storage, read_write> prefix_sums: array<u32>;
@group(0) @binding(3) var<storage, read> input_col: array<i32>;
@group(0) @binding(4) var<storage, read_write> output_col: array<i32>;

// ── Pass 1: compute prefix sums (inclusive scan over popcount per word) ────────
//
// Each thread handles one mask word.
// We compute a simple serial inclusive scan — for the word counts we have
// (up to ~2048 words for 65536 rows), a single-pass serial scan in a
// single workgroup is efficient enough.
//
// Thread 0 performs the full scan; other threads idle. For production-scale
// use a parallel workgroup prefix scan would be faster, but correctness first.

@compute @workgroup_size(1)
fn prefix_sum(@builtin(global_invocation_id) gid: vec3<u32>) {
    if gid.x != 0u { return; }

    var running: u32 = 0u;
    for (var w: u32 = 0u; w < params.num_words; w++) {
        running += countOneBits(mask[w]);
        prefix_sums[w] = running;
    }
}

// ── Pass 2: scatter passing rows into the output buffer ────────────────────────
//
// Each thread handles one input row.
// If the row passes the filter (its mask bit is set), compute its output index
// using the prefix sum, then write the value.
//
// Output index for row `idx` in mask word `w = idx / 32` at bit `b = idx % 32`:
//   base  = prefix_sums[w - 1]  (0 if w == 0)
//   local = popcount(mask[w] & ((1 << b) - 1))   ← bits before ours within the word
//   out_idx = base + local

@compute @workgroup_size(256)
fn scatter(@builtin(global_invocation_id) gid: vec3<u32>) {
    let idx = gid.x;
    if idx >= params.num_rows { return; }

    let w = idx / 32u;
    let b = idx % 32u;

    // Check if this row passes
    let word = mask[w];
    if ((word >> b) & 1u) == 0u { return; }

    // Compute output index
    let base: u32 = select(0u, prefix_sums[w - 1u], w > 0u);
    // Count set bits in this word strictly before position b
    let lower_bits = word & ((1u << b) - 1u);
    let local_offset = countOneBits(lower_bits);
    let out_idx = base + local_offset;

    // Write value(s) to output
    if params.is_wide == 1u {
        // i64: two i32 words per element
        output_col[out_idx * 2u]      = input_col[idx * 2u];
        output_col[out_idx * 2u + 1u] = input_col[idx * 2u + 1u];
    } else {
        output_col[out_idx] = input_col[idx];
    }
}
