// WGSL Filter Shader for LTSeq GPU acceleration
//
// Evaluates comparison predicates (column vs literal) and produces a bitmask.
// Each thread processes one element, writing to a packed u32 bitmask output
// where bit N=1 means row N passes the filter.
//
// Supported types: i32 (native), i64 (emulated via two i32 words), f32 (native)
// Supported ops:   eq=0, neq=1, lt=2, lte=3, gt=4, gte=5
//
// Bind groups:
//   @group(0) @binding(0)  params    — uniform: op, literal, num_rows, column type
//   @group(0) @binding(1)  col_data  — storage: column values (i32 or paired i32 for i64)
//   @group(0) @binding(2)  out_mask  — storage: output bitmask (u32 packed, 32 rows/word)

struct Params {
    op: u32,         // 0=eq, 1=neq, 2=lt, 3=lte, 4=gt, 5=gte
    col_type: u32,   // 0=i32, 1=i64, 2=f32
    num_rows: u32,
    _pad: u32,
    literal_lo: i32, // For i32/f32: the literal value. For i64: low 32 bits.
    literal_hi: i32, // For i64: high 32 bits (signed). Unused for i32/f32.
    _pad2: i32,
    _pad3: i32,
}

@group(0) @binding(0) var<uniform> params: Params;
@group(0) @binding(1) var<storage, read> col_data: array<i32>;
@group(0) @binding(2) var<storage, read_write> out_mask: array<atomic<u32>>;

// ── i32 comparison ──────────────────────────────────────────────────
fn compare_i32(val: i32, lit: i32, op: u32) -> bool {
    switch op {
        case 0u: { return val == lit; }   // eq
        case 1u: { return val != lit; }   // neq
        case 2u: { return val < lit; }    // lt
        case 3u: { return val <= lit; }   // lte
        case 4u: { return val > lit; }    // gt
        case 5u: { return val >= lit; }   // gte
        default: { return false; }
    }
}

// ── f32 comparison ──────────────────────────────────────────────────
fn compare_f32(val: f32, lit: f32, op: u32) -> bool {
    switch op {
        case 0u: { return val == lit; }
        case 1u: { return val != lit; }
        case 2u: { return val < lit; }
        case 3u: { return val <= lit; }
        case 4u: { return val > lit; }
        case 5u: { return val >= lit; }
        default: { return false; }
    }
}

// ── i64 comparison (emulated via two i32 words) ─────────────────────
// Layout: col_data[2*i] = high word (signed), col_data[2*i+1] = low word (unsigned)
// Comparison: first compare high words (signed), if equal compare low words (unsigned).
fn compare_i64(val_hi: i32, val_lo: i32, lit_hi: i32, lit_lo: i32, op: u32) -> bool {
    // Convert low words to unsigned for proper comparison
    let v_lo = bitcast<u32>(val_lo);
    let l_lo = bitcast<u32>(lit_lo);

    switch op {
        case 0u: { // eq
            return val_hi == lit_hi && v_lo == l_lo;
        }
        case 1u: { // neq
            return val_hi != lit_hi || v_lo != l_lo;
        }
        case 2u: { // lt
            if val_hi != lit_hi { return val_hi < lit_hi; }
            return v_lo < l_lo;
        }
        case 3u: { // lte
            if val_hi != lit_hi { return val_hi < lit_hi; }
            return v_lo <= l_lo;
        }
        case 4u: { // gt
            if val_hi != lit_hi { return val_hi > lit_hi; }
            return v_lo > l_lo;
        }
        case 5u: { // gte
            if val_hi != lit_hi { return val_hi > lit_hi; }
            return v_lo >= l_lo;
        }
        default: { return false; }
    }
}

@compute @workgroup_size(256)
fn main(@builtin(global_invocation_id) gid: vec3<u32>) {
    let idx = gid.x;
    if idx >= params.num_rows {
        return;
    }

    var passes = false;

    switch params.col_type {
        case 0u: { // i32
            let val = col_data[idx];
            passes = compare_i32(val, params.literal_lo, params.op);
        }
        case 1u: { // i64 — two i32 words per element (little-endian: lo then hi)
            let val_lo = col_data[idx * 2u];
            let val_hi = col_data[idx * 2u + 1u];
            passes = compare_i64(val_hi, val_lo, params.literal_hi, params.literal_lo, params.op);
        }
        case 2u: { // f32 — reinterpret i32 bits as f32
            let val = bitcast<f32>(col_data[idx]);
            let lit = bitcast<f32>(params.literal_lo);
            passes = compare_f32(val, lit, params.op);
        }
        default: {}
    }

    if passes {
        let word_idx = idx / 32u;
        let bit_idx = idx % 32u;
        atomicOr(&out_mask[word_idx], 1u << bit_idx);
    }
}
