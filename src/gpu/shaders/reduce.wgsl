// WGSL Reduction Shaders for LTSeq GPU-accelerated GROUP BY aggregation
//
// Overview:
//   LTSeq exploits sorted input for efficient group aggregation. When data is
//   sorted by the GROUP BY key column, identical groups form contiguous segments.
//   We use two kernels:
//
//   Kernel 1: detect_boundaries
//     Compares consecutive rows on the group key column. Outputs a packed u32
//     bitmask where bit N=1 means row N is the START of a new group (row 0 is
//     always a group start).
//
//   Kernel 2: segmented_reduce
//     Given segment start positions and lengths (computed on CPU from the bitmask),
//     each workgroup handles exactly one segment. Threads cooperate via shared
//     memory tree reduction to compute sum/count/min/max for that segment.
//     Supports i32 (native) and i64 (emulated via two i32 words). f32 is handled
//     by reinterpreting bits (stored as i32) and converting on CPU.
//
// Aggregate operations:
//   0 = sum    (i32 or two-word i64)
//   1 = count  (always i32, output: i32 count of rows in segment)
//   2 = min    (i32 or two-word i64)
//   3 = max    (i32 or two-word i64)
//
// Bind group layouts are described per kernel below.

// ══════════════════════════════════════════════════════════════════════════════
//  Kernel 1: detect_boundaries
//  Outputs packed u32 bitmask: bit N=1 → row N is a group start
//
//  Bindings:
//   @binding(0) boundary_params  — uniform: key_type(0=i32,1=i64), num_rows
//   @binding(1) key_data         — storage: key column values (i32 or i32x2 for i64)
//   @binding(2) out_mask         — storage: output bitmask (atomic<u32>)
// ══════════════════════════════════════════════════════════════════════════════

struct BoundaryParams {
    key_type: u32,   // 0 = i32, 1 = i64
    num_rows: u32,
    _pad0: u32,
    _pad1: u32,
}

@group(0) @binding(0) var<uniform> boundary_params: BoundaryParams;
@group(0) @binding(1) var<storage, read> key_data: array<i32>;
@group(0) @binding(2) var<storage, read_write> boundary_mask: array<atomic<u32>>;

fn keys_equal_i32(a: i32, b: i32) -> bool {
    return a == b;
}

fn keys_equal_i64(a_lo: i32, a_hi: i32, b_lo: i32, b_hi: i32) -> bool {
    return a_hi == b_hi && a_lo == b_lo;
}

@compute @workgroup_size(256)
fn detect_boundaries(@builtin(global_invocation_id) gid: vec3<u32>) {
    let idx = gid.x;
    if idx >= boundary_params.num_rows {
        return;
    }

    // Row 0 is always a boundary (start of first group)
    var is_boundary = (idx == 0u);

    if !is_boundary {
        switch boundary_params.key_type {
            case 0u: { // i32 — one word per element
                let prev = key_data[idx - 1u];
                let curr = key_data[idx];
                is_boundary = !keys_equal_i32(prev, curr);
            }
            case 1u: { // i64 — two i32 words per element (little-endian: lo, hi)
                let prev_lo = key_data[(idx - 1u) * 2u];
                let prev_hi = key_data[(idx - 1u) * 2u + 1u];
                let curr_lo = key_data[idx * 2u];
                let curr_hi = key_data[idx * 2u + 1u];
                is_boundary = !keys_equal_i64(prev_lo, prev_hi, curr_lo, curr_hi);
            }
            default: {}
        }
    }

    if is_boundary {
        let word_idx = idx / 32u;
        let bit_idx = idx % 32u;
        atomicOr(&boundary_mask[word_idx], 1u << bit_idx);
    }
}

// ══════════════════════════════════════════════════════════════════════════════
//  Kernel 2: segmented_reduce_i32
//  One workgroup per segment. Each thread processes multiple elements via loop.
//  Then a tree reduction collapses 256 accumulators to 1 result.
//
//  Bindings:
//   @binding(0) reduce_params   — uniform: agg_op(0-3), num_segments
//   @binding(1) value_data      — storage: i32 value column
//   @binding(2) seg_starts      — storage: segment start row indices (u32)
//   @binding(3) seg_lengths     — storage: segment lengths in rows (u32)
//   @binding(4) out_results     — storage: one i32 result per segment
// ══════════════════════════════════════════════════════════════════════════════

struct ReduceParams {
    agg_op: u32,       // 0=sum, 1=count, 2=min, 3=max
    num_segments: u32,
    _pad0: u32,
    _pad1: u32,
}

@group(0) @binding(0) var<uniform> reduce_params_i32: ReduceParams;
@group(0) @binding(1) var<storage, read> value_data_i32: array<i32>;
@group(0) @binding(2) var<storage, read> seg_starts_i32: array<u32>;
@group(0) @binding(3) var<storage, read> seg_lengths_i32: array<u32>;
@group(0) @binding(4) var<storage, read_write> out_results_i32: array<i32>;

var<workgroup> shared_i32: array<i32, 256>;

@compute @workgroup_size(256)
fn segmented_reduce_i32(
    @builtin(workgroup_id) wg_id: vec3<u32>,
    @builtin(local_invocation_id) local_id: vec3<u32>,
) {
    let seg_idx = wg_id.x;
    if seg_idx >= reduce_params_i32.num_segments {
        return;
    }

    let seg_start = seg_starts_i32[seg_idx];
    let seg_len   = seg_lengths_i32[seg_idx];
    let lid       = local_id.x;  // 0..255
    let op        = reduce_params_i32.agg_op;

    // Identity values per operation
    var acc: i32;
    switch op {
        case 0u: { acc = 0i; }                                // sum → 0
        case 1u: { acc = 0i; }                                // count → 0
        case 2u: { acc = 2147483647i; }                       // min → i32::MAX
        case 3u: { acc = bitcast<i32>(0x80000000u); }         // max → i32::MIN
        default: { acc = 0i; }
    }

    // Each thread loops over its share of the segment
    var i: u32 = lid;
    loop {
        if i >= seg_len { break; }
        let val = value_data_i32[seg_start + i];
        switch op {
            case 0u: { acc = acc + val; }               // sum
            case 1u: { acc = acc + 1i; }                // count (ignore value)
            case 2u: { if val < acc { acc = val; } }    // min
            case 3u: { if val > acc { acc = val; } }    // max
            default: {}
        }
        i = i + 256u;
    }

    shared_i32[lid] = acc;
    workgroupBarrier();

    // Tree reduction: 256 → 128 → 64 → 32 → 16 → 8 → 4 → 2 → 1
    var stride: u32 = 128u;
    loop {
        if stride == 0u { break; }
        if lid < stride {
            let other = shared_i32[lid + stride];
            switch op {
                case 0u: { shared_i32[lid] = shared_i32[lid] + other; }
                case 1u: { shared_i32[lid] = shared_i32[lid] + other; }
                case 2u: { if other < shared_i32[lid] { shared_i32[lid] = other; } }
                case 3u: { if other > shared_i32[lid] { shared_i32[lid] = other; } }
                default: {}
            }
        }
        workgroupBarrier();
        stride = stride >> 1u;
    }

    if lid == 0u {
        out_results_i32[seg_idx] = shared_i32[0];
    }
}

// ══════════════════════════════════════════════════════════════════════════════
//  Kernel 3: segmented_reduce_i64
//  Same structure as i32 but each element is two i32 words (lo, hi).
//  i64 addition/min/max via two-word arithmetic.
//
//  Bindings: same layout as i32 variant, but value_data_i64 holds i32 pairs
//  and out_results_i64 holds two i32 words per segment (lo at [2*seg], hi at [2*seg+1]).
// ══════════════════════════════════════════════════════════════════════════════

@group(0) @binding(0) var<uniform> reduce_params_i64: ReduceParams;
@group(0) @binding(1) var<storage, read> value_data_i64: array<i32>;
@group(0) @binding(2) var<storage, read> seg_starts_i64: array<u32>;
@group(0) @binding(3) var<storage, read> seg_lengths_i64: array<u32>;
@group(0) @binding(4) var<storage, read_write> out_results_i64: array<i32>;

// Shared memory: 256 slots × 2 words each
var<workgroup> shared_lo: array<i32, 256>;
var<workgroup> shared_hi: array<i32, 256>;

fn i64_lt(a_lo: i32, a_hi: i32, b_lo: i32, b_hi: i32) -> bool {
    if a_hi != b_hi { return a_hi < b_hi; }
    return bitcast<u32>(a_lo) < bitcast<u32>(b_lo);
}

fn i64_add_lo(a_lo: i32, b_lo: i32) -> i32 {
    // Wrapping add of low words (interpreted as u32 then recast to i32)
    let a = bitcast<u32>(a_lo);
    let b = bitcast<u32>(b_lo);
    return bitcast<i32>(a + b);
}

fn i64_add_hi(a_lo: i32, a_hi: i32, b_lo: i32, b_hi: i32) -> i32 {
    // Add high words, plus carry from low addition
    let a_l = bitcast<u32>(a_lo);
    let b_l = bitcast<u32>(b_lo);
    let sum_l = a_l + b_l;
    let carry = select(0i, 1i, sum_l < a_l);
    return a_hi + b_hi + carry;
}

@compute @workgroup_size(256)
fn segmented_reduce_i64(
    @builtin(workgroup_id) wg_id: vec3<u32>,
    @builtin(local_invocation_id) local_id: vec3<u32>,
) {
    let seg_idx = wg_id.x;
    if seg_idx >= reduce_params_i64.num_segments {
        return;
    }

    let seg_start = seg_starts_i64[seg_idx];
    let seg_len   = seg_lengths_i64[seg_idx];
    let lid       = local_id.x;
    let op        = reduce_params_i64.agg_op;

    // Identity values
    var acc_lo: i32;
    var acc_hi: i32;
    switch op {
        case 0u: { acc_lo = 0i; acc_hi = 0i; }                          // sum
        case 1u: { acc_lo = 0i; acc_hi = 0i; }                          // count (use i32 logic)
        case 2u: { acc_lo = -1i; acc_hi = 2147483647i; }                // min → i64::MAX
        case 3u: { acc_lo = 0i; acc_hi = bitcast<i32>(0x80000000u); }    // max → i64::MIN
        default: { acc_lo = 0i; acc_hi = 0i; }
    }

    var i: u32 = lid;
    loop {
        if i >= seg_len { break; }
        let val_lo = value_data_i64[(seg_start + i) * 2u];
        let val_hi = value_data_i64[(seg_start + i) * 2u + 1u];
        switch op {
            case 0u: { // sum
                let new_lo = i64_add_lo(acc_lo, val_lo);
                acc_hi = i64_add_hi(acc_lo, acc_hi, val_lo, val_hi);
                acc_lo = new_lo;
            }
            case 1u: { // count (just add 1 as i32)
                acc_lo = acc_lo + 1i;
            }
            case 2u: { // min
                if i64_lt(val_lo, val_hi, acc_lo, acc_hi) {
                    acc_lo = val_lo;
                    acc_hi = val_hi;
                }
            }
            case 3u: { // max
                if i64_lt(acc_lo, acc_hi, val_lo, val_hi) {
                    acc_lo = val_lo;
                    acc_hi = val_hi;
                }
            }
            default: {}
        }
        i = i + 256u;
    }

    shared_lo[lid] = acc_lo;
    shared_hi[lid] = acc_hi;
    workgroupBarrier();

    var stride: u32 = 128u;
    loop {
        if stride == 0u { break; }
        if lid < stride {
            let o_lo = shared_lo[lid + stride];
            let o_hi = shared_hi[lid + stride];
            switch op {
                case 0u: { // sum
                    let new_lo = i64_add_lo(shared_lo[lid], o_lo);
                    shared_hi[lid] = i64_add_hi(shared_lo[lid], shared_hi[lid], o_lo, o_hi);
                    shared_lo[lid] = new_lo;
                }
                case 1u: { // count
                    shared_lo[lid] = shared_lo[lid] + o_lo;
                }
                case 2u: { // min
                    if i64_lt(o_lo, o_hi, shared_lo[lid], shared_hi[lid]) {
                        shared_lo[lid] = o_lo;
                        shared_hi[lid] = o_hi;
                    }
                }
                case 3u: { // max
                    if i64_lt(shared_lo[lid], shared_hi[lid], o_lo, o_hi) {
                        shared_lo[lid] = o_lo;
                        shared_hi[lid] = o_hi;
                    }
                }
                default: {}
            }
        }
        workgroupBarrier();
        stride = stride >> 1u;
    }

    if lid == 0u {
        out_results_i64[seg_idx * 2u]      = shared_lo[0];
        out_results_i64[seg_idx * 2u + 1u] = shared_hi[0];
    }
}
