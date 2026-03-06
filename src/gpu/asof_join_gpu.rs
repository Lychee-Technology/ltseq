//! GPU-accelerated as-of join via parallel binary search.
//!
//! # Algorithm
//!
//! For each left row, perform a binary search on the sorted right time column
//! to find the matching row. Each left row's search is independent — GPU launches
//! all searches in parallel (one thread per left row).
//!
//! ## Direction Semantics
//!
//! * `backward`: Find largest right.time where right.time <= left.time
//! * `forward`:  Find smallest right.time where right.time >= left.time
//! * `nearest`:  Find closest right.time (backward bias on ties)
//!
//! ## Performance
//!
//! O(N log M) total work, O(log M) span, where N = left rows, M = right rows.
//! GPU parallelism makes this effectively O(log M) wall-clock time.
//!
//! # CUDA Kernels
//!
//! - `asof_backward_i64`: Binary search for largest right <= left
//! - `asof_forward_i64`:  Binary search for smallest right >= left
//! - `asof_nearest_i64`:  Binary search for nearest (backward bias on ties)

use cudarc::driver::safe::LaunchConfig;
use cudarc::driver::PushKernelArg;
use cudarc::nvrtc::compile_ptx;
use std::sync::{Arc, OnceLock};

use super::{get_context, get_stream, CUDA_BLOCK_SIZE, GPU_MIN_ROWS_THRESHOLD};

// ============================================================================
// CUDA Kernel Source
// ============================================================================

/// CUDA kernels for as-of join binary search.
///
/// Each kernel processes one left row per thread:
/// - `left_times`: sorted left time column (N elements)
/// - `right_times`: sorted right time column (M elements)
/// - `out_indices`: output match index per left row (i32, -1 = no match)
/// - `n_left`: number of left rows
/// - `n_right`: number of right rows
///
/// We use i32 for output indices (-1 sentinel for "no match") since
/// the right table is unlikely to exceed 2B rows in practice.
const ASOF_JOIN_KERNEL_SRC: &str = r#"
// Backward: find largest index where right_times[idx] <= target
extern "C" __global__ void asof_backward_i64(
    const long long* __restrict__ left_times,
    const long long* __restrict__ right_times,
    int* __restrict__ out_indices,
    const unsigned int n_left,
    const unsigned int n_right
) {
    unsigned int tid = blockIdx.x * blockDim.x + threadIdx.x;
    if (tid >= n_left) return;

    long long target = left_times[tid];

    // Binary search: find partition_point where right_times[idx] <= target
    // i.e., find the first index where right_times[idx] > target
    unsigned int lo = 0;
    unsigned int hi = n_right;
    while (lo < hi) {
        unsigned int mid = lo + (hi - lo) / 2;
        if (right_times[mid] <= target) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    // lo = first index where right_times[idx] > target
    // So the match is lo - 1 (the largest index where right_times[idx] <= target)
    out_indices[tid] = (lo > 0) ? (int)(lo - 1) : -1;
}

// Forward: find smallest index where right_times[idx] >= target
extern "C" __global__ void asof_forward_i64(
    const long long* __restrict__ left_times,
    const long long* __restrict__ right_times,
    int* __restrict__ out_indices,
    const unsigned int n_left,
    const unsigned int n_right
) {
    unsigned int tid = blockIdx.x * blockDim.x + threadIdx.x;
    if (tid >= n_left) return;

    long long target = left_times[tid];

    // Binary search: find partition_point where right_times[idx] < target
    // i.e., find the first index where right_times[idx] >= target
    unsigned int lo = 0;
    unsigned int hi = n_right;
    while (lo < hi) {
        unsigned int mid = lo + (hi - lo) / 2;
        if (right_times[mid] < target) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    // lo = first index where right_times[idx] >= target
    out_indices[tid] = (lo < n_right) ? (int)lo : -1;
}

// Nearest: find closest match (backward bias on ties)
extern "C" __global__ void asof_nearest_i64(
    const long long* __restrict__ left_times,
    const long long* __restrict__ right_times,
    int* __restrict__ out_indices,
    const unsigned int n_left,
    const unsigned int n_right
) {
    unsigned int tid = blockIdx.x * blockDim.x + threadIdx.x;
    if (tid >= n_left) return;

    long long target = left_times[tid];

    // Find backward match: largest right_times[idx] <= target
    unsigned int lo = 0;
    unsigned int hi = n_right;
    while (lo < hi) {
        unsigned int mid = lo + (hi - lo) / 2;
        if (right_times[mid] <= target) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    int back_idx = (lo > 0) ? (int)(lo - 1) : -1;

    // Find forward match: smallest right_times[idx] >= target
    lo = 0;
    hi = n_right;
    while (lo < hi) {
        unsigned int mid = lo + (hi - lo) / 2;
        if (right_times[mid] < target) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    int fwd_idx = (lo < n_right) ? (int)lo : -1;

    // Pick the closer one (backward bias on ties)
    if (back_idx < 0 && fwd_idx < 0) {
        out_indices[tid] = -1;
    } else if (back_idx < 0) {
        out_indices[tid] = fwd_idx;
    } else if (fwd_idx < 0) {
        out_indices[tid] = back_idx;
    } else {
        long long diff_back = target - right_times[back_idx];
        long long diff_fwd  = right_times[fwd_idx] - target;
        out_indices[tid] = (diff_back <= diff_fwd) ? back_idx : fwd_idx;
    }
}
"#;

// ============================================================================
// Module initialization
// ============================================================================

/// Compiled CUDA module for as-of join kernels.
static ASOF_MODULE: OnceLock<Option<Arc<cudarc::driver::safe::CudaModule>>> = OnceLock::new();

/// Get or compile the as-of join CUDA module.
fn get_asof_module() -> Option<Arc<cudarc::driver::safe::CudaModule>> {
    ASOF_MODULE
        .get_or_init(|| {
            let ctx = get_context()?;
            let ptx = compile_ptx(ASOF_JOIN_KERNEL_SRC).ok()?;
            let module = ctx.load_module(ptx).ok()?;
            Some(module)
        })
        .clone()
}

// ============================================================================
// GPU execution
// ============================================================================

/// Direction for as-of join matching.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AsofDirection {
    Backward,
    Forward,
    Nearest,
}

impl AsofDirection {
    /// Parse direction from string (matching the Python API).
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "backward" => Some(Self::Backward),
            "forward" => Some(Self::Forward),
            "nearest" => Some(Self::Nearest),
            _ => None,
        }
    }

    /// Get the CUDA kernel function name for this direction.
    fn kernel_name(&self) -> &'static str {
        match self {
            Self::Backward => "asof_backward_i64",
            Self::Forward => "asof_forward_i64",
            Self::Nearest => "asof_nearest_i64",
        }
    }
}

/// Perform GPU-accelerated as-of join binary search.
///
/// Takes sorted time columns from left and right tables, performs parallel
/// binary search on GPU, and returns match indices (None = no match).
///
/// Returns `None` if GPU is unavailable, enabling transparent fallback to CPU.
pub fn gpu_asof_join(
    left_times: &[i64],
    right_times: &[i64],
    direction: AsofDirection,
) -> Option<Vec<Option<usize>>> {
    let n_left = left_times.len();
    let n_right = right_times.len();

    if n_left == 0 {
        return Some(Vec::new());
    }
    if n_right == 0 {
        return Some(vec![None; n_left]);
    }

    let module = get_asof_module()?;
    let stream = get_stream()?;
    let kernel_name = direction.kernel_name();
    let func = module.load_function(kernel_name).ok()?;

    // Upload time arrays to GPU
    let d_left = stream.clone_htod(left_times).ok()?;
    let d_right = stream.clone_htod(right_times).ok()?;

    // Allocate output: i32 indices (-1 = no match)
    let init_indices = vec![-1i32; n_left];
    let d_out = stream.clone_htod(&init_indices).ok()?;

    // Launch kernel
    let grid = super::grid_size(n_left as u32);
    let cfg = LaunchConfig {
        grid_dim: (grid, 1, 1),
        block_dim: (CUDA_BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };

    let n_left_u32 = n_left as u32;
    let n_right_u32 = n_right as u32;

    unsafe {
        let mut builder = stream.launch_builder(&func);
        builder.arg(&d_left);
        builder.arg(&d_right);
        builder.arg(&d_out);
        builder.arg(&n_left_u32);
        builder.arg(&n_right_u32);
        builder.launch(cfg).ok()?;
    }

    // Download results
    let result_indices = stream.clone_dtoh(&d_out).ok()?;

    // Convert i32 indices to Option<usize>
    let matched: Vec<Option<usize>> = result_indices
        .iter()
        .map(|&idx| if idx < 0 { None } else { Some(idx as usize) })
        .collect();

    Some(matched)
}

/// Check if GPU as-of join should be used for the given data sizes.
///
/// Returns true if GPU is available and either table exceeds the row threshold.
pub fn should_use_gpu_asof(n_left: usize, n_right: usize) -> bool {
    // For as-of join, the benefit comes from parallelizing the N binary searches.
    // Use GPU when left table (number of searches) is large enough.
    #[cfg(feature = "gpu")]
    {
        n_left >= GPU_MIN_ROWS_THRESHOLD && super::is_gpu_available()
    }
    #[cfg(not(feature = "gpu"))]
    {
        let _ = (n_left, n_right);
        false
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Direction parsing ----

    #[test]
    fn test_direction_from_str() {
        assert_eq!(
            AsofDirection::from_str("backward"),
            Some(AsofDirection::Backward)
        );
        assert_eq!(
            AsofDirection::from_str("forward"),
            Some(AsofDirection::Forward)
        );
        assert_eq!(
            AsofDirection::from_str("nearest"),
            Some(AsofDirection::Nearest)
        );
        assert_eq!(AsofDirection::from_str("invalid"), None);
    }

    #[test]
    fn test_direction_kernel_name() {
        assert_eq!(AsofDirection::Backward.kernel_name(), "asof_backward_i64");
        assert_eq!(AsofDirection::Forward.kernel_name(), "asof_forward_i64");
        assert_eq!(AsofDirection::Nearest.kernel_name(), "asof_nearest_i64");
    }

    // ---- CPU reference implementation for test validation ----

    fn cpu_asof_backward(left: &[i64], right: &[i64]) -> Vec<Option<usize>> {
        left.iter()
            .map(|&target| {
                let idx = right.partition_point(|&t| t <= target);
                if idx == 0 {
                    None
                } else {
                    Some(idx - 1)
                }
            })
            .collect()
    }

    fn cpu_asof_forward(left: &[i64], right: &[i64]) -> Vec<Option<usize>> {
        left.iter()
            .map(|&target| {
                let idx = right.partition_point(|&t| t < target);
                if idx >= right.len() {
                    None
                } else {
                    Some(idx)
                }
            })
            .collect()
    }

    fn cpu_asof_nearest(left: &[i64], right: &[i64]) -> Vec<Option<usize>> {
        left.iter()
            .map(|&target| {
                let back = {
                    let idx = right.partition_point(|&t| t <= target);
                    if idx == 0 {
                        None
                    } else {
                        Some(idx - 1)
                    }
                };
                let fwd = {
                    let idx = right.partition_point(|&t| t < target);
                    if idx >= right.len() {
                        None
                    } else {
                        Some(idx)
                    }
                };
                match (back, fwd) {
                    (None, None) => None,
                    (Some(b), None) => Some(b),
                    (None, Some(f)) => Some(f),
                    (Some(b), Some(f)) => {
                        let diff_back = target - right[b];
                        let diff_fwd = right[f] - target;
                        if diff_back <= diff_fwd {
                            Some(b)
                        } else {
                            Some(f)
                        }
                    }
                }
            })
            .collect()
    }

    // ---- Unit tests for CPU reference (validate test helpers) ----

    #[test]
    fn test_cpu_backward_basic() {
        let left = vec![1000, 1005, 1010, 1015, 1020];
        let right = vec![998, 1002, 1008, 1012, 1018];
        let result = cpu_asof_backward(&left, &right);
        // 1000 -> 998 (idx 0), 1005 -> 1002 (idx 1), 1010 -> 1008 (idx 2),
        // 1015 -> 1012 (idx 3), 1020 -> 1018 (idx 4)
        assert_eq!(result, vec![Some(0), Some(1), Some(2), Some(3), Some(4)]);
    }

    #[test]
    fn test_cpu_forward_basic() {
        let left = vec![1000, 1005, 1010, 1015, 1020];
        let right = vec![998, 1002, 1008, 1012, 1018];
        let result = cpu_asof_forward(&left, &right);
        // 1000 -> 1002 (idx 1), 1005 -> 1008 (idx 2), 1010 -> 1010 -> 1012 (idx 3),
        // 1015 -> 1018 (idx 4), 1020 -> None (no right >= 1020)
        assert_eq!(result, vec![Some(1), Some(2), Some(3), Some(4), None]);
    }

    #[test]
    fn test_cpu_nearest_tie_backward_bias() {
        let left = vec![1005];
        let right = vec![1000, 1010]; // Both 5 away
        let result = cpu_asof_nearest(&left, &right);
        assert_eq!(result, vec![Some(0)]); // backward bias -> 1000
    }

    #[test]
    fn test_cpu_nearest_closer_forward() {
        let left = vec![1005];
        let right = vec![1000, 1006]; // 1006 is closer (1 away vs 5)
        let result = cpu_asof_nearest(&left, &right);
        assert_eq!(result, vec![Some(1)]); // 1006 is closer
    }

    #[test]
    fn test_cpu_backward_no_match() {
        let left = vec![500];
        let right = vec![1000, 2000];
        let result = cpu_asof_backward(&left, &right);
        assert_eq!(result, vec![None]);
    }

    #[test]
    fn test_cpu_forward_no_match() {
        let left = vec![3000];
        let right = vec![1000, 2000];
        let result = cpu_asof_forward(&left, &right);
        assert_eq!(result, vec![None]);
    }

    #[test]
    fn test_cpu_backward_exact_match() {
        let left = vec![1000];
        let right = vec![1000];
        let result = cpu_asof_backward(&left, &right);
        assert_eq!(result, vec![Some(0)]);
    }

    #[test]
    fn test_cpu_forward_exact_match() {
        let left = vec![1000];
        let right = vec![1000];
        let result = cpu_asof_forward(&left, &right);
        assert_eq!(result, vec![Some(0)]);
    }

    #[test]
    fn test_cpu_backward_empty_right() {
        let left = vec![1000, 2000];
        let right: Vec<i64> = vec![];
        let result = cpu_asof_backward(&left, &right);
        assert_eq!(result, vec![None, None]);
    }

    #[test]
    fn test_cpu_forward_empty_left() {
        let left: Vec<i64> = vec![];
        let right = vec![1000, 2000];
        let result = cpu_asof_forward(&left, &right);
        assert_eq!(result, Vec::<Option<usize>>::new());
    }

    // ---- should_use_gpu tests ----

    #[test]
    fn test_should_use_gpu_below_threshold() {
        // Below GPU_MIN_ROWS_THRESHOLD (100_000), should NOT use GPU
        assert!(!should_use_gpu_asof(1000, 1000));
    }

    #[test]
    fn test_should_use_gpu_at_threshold() {
        // At threshold — depends on GPU availability (which we can't control in tests)
        // Just verify it doesn't panic
        let _ = should_use_gpu_asof(GPU_MIN_ROWS_THRESHOLD, 1000);
    }

    // ---- GPU execution tests (run only when GPU is available) ----

    #[test]
    fn test_gpu_asof_backward_basic() {
        let left = vec![1000i64, 1005, 1010, 1015, 1020];
        let right = vec![998i64, 1002, 1008, 1012, 1018];

        match gpu_asof_join(&left, &right, AsofDirection::Backward) {
            Some(result) => {
                let expected = cpu_asof_backward(&left, &right);
                assert_eq!(result, expected, "GPU backward should match CPU reference");
            }
            None => {
                // GPU not available, skip
                eprintln!("GPU not available, skipping test_gpu_asof_backward_basic");
            }
        }
    }

    #[test]
    fn test_gpu_asof_forward_basic() {
        let left = vec![1000i64, 1005, 1010, 1015, 1020];
        let right = vec![998i64, 1002, 1008, 1012, 1018];

        match gpu_asof_join(&left, &right, AsofDirection::Forward) {
            Some(result) => {
                let expected = cpu_asof_forward(&left, &right);
                assert_eq!(result, expected, "GPU forward should match CPU reference");
            }
            None => {
                eprintln!("GPU not available, skipping test_gpu_asof_forward_basic");
            }
        }
    }

    #[test]
    fn test_gpu_asof_nearest_basic() {
        let left = vec![1000i64, 1005, 1010, 1015, 1020];
        let right = vec![998i64, 1002, 1008, 1012, 1018];

        match gpu_asof_join(&left, &right, AsofDirection::Nearest) {
            Some(result) => {
                let expected = cpu_asof_nearest(&left, &right);
                assert_eq!(result, expected, "GPU nearest should match CPU reference");
            }
            None => {
                eprintln!("GPU not available, skipping test_gpu_asof_nearest_basic");
            }
        }
    }

    #[test]
    fn test_gpu_asof_backward_no_match() {
        let left = vec![500i64];
        let right = vec![1000i64, 2000];

        match gpu_asof_join(&left, &right, AsofDirection::Backward) {
            Some(result) => {
                assert_eq!(result, vec![None]);
            }
            None => {
                eprintln!("GPU not available, skipping test_gpu_asof_backward_no_match");
            }
        }
    }

    #[test]
    fn test_gpu_asof_forward_no_match() {
        let left = vec![3000i64];
        let right = vec![1000i64, 2000];

        match gpu_asof_join(&left, &right, AsofDirection::Forward) {
            Some(result) => {
                assert_eq!(result, vec![None]);
            }
            None => {
                eprintln!("GPU not available, skipping test_gpu_asof_forward_no_match");
            }
        }
    }

    #[test]
    fn test_gpu_asof_nearest_tie_backward_bias() {
        let left = vec![1005i64];
        let right = vec![1000i64, 1010];

        match gpu_asof_join(&left, &right, AsofDirection::Nearest) {
            Some(result) => {
                assert_eq!(
                    result,
                    vec![Some(0)],
                    "Nearest with tie should have backward bias"
                );
            }
            None => {
                eprintln!("GPU not available, skipping test_gpu_asof_nearest_tie_backward_bias");
            }
        }
    }

    #[test]
    fn test_gpu_asof_nearest_closer_forward() {
        let left = vec![1005i64];
        let right = vec![1000i64, 1006];

        match gpu_asof_join(&left, &right, AsofDirection::Nearest) {
            Some(result) => {
                assert_eq!(
                    result,
                    vec![Some(1)],
                    "Nearest should pick closer forward match"
                );
            }
            None => {
                eprintln!("GPU not available, skipping test_gpu_asof_nearest_closer_forward");
            }
        }
    }

    #[test]
    fn test_gpu_asof_empty_right() {
        let left = vec![1000i64, 2000];
        let right: Vec<i64> = vec![];

        match gpu_asof_join(&left, &right, AsofDirection::Backward) {
            Some(result) => {
                assert_eq!(result, vec![None, None]);
            }
            None => {
                eprintln!("GPU not available, skipping test_gpu_asof_empty_right");
            }
        }
    }

    #[test]
    fn test_gpu_asof_empty_left() {
        let left: Vec<i64> = vec![];
        let right = vec![1000i64, 2000];

        match gpu_asof_join(&left, &right, AsofDirection::Backward) {
            Some(result) => {
                assert!(result.is_empty());
            }
            None => {
                eprintln!("GPU not available, skipping test_gpu_asof_empty_left");
            }
        }
    }

    #[test]
    fn test_gpu_asof_exact_match() {
        let left = vec![1000i64];
        let right = vec![1000i64];

        // Backward: exact match should work
        if let Some(result) = gpu_asof_join(&left, &right, AsofDirection::Backward) {
            assert_eq!(result, vec![Some(0)]);
        }

        // Forward: exact match should work
        if let Some(result) = gpu_asof_join(&left, &right, AsofDirection::Forward) {
            assert_eq!(result, vec![Some(0)]);
        }

        // Nearest: exact match should work
        if let Some(result) = gpu_asof_join(&left, &right, AsofDirection::Nearest) {
            assert_eq!(result, vec![Some(0)]);
        }
    }

    #[test]
    fn test_gpu_asof_large_right_table() {
        // Test with larger right table to exercise binary search more thoroughly
        let left = vec![50i64, 150, 250, 350, 450, 550, 650, 750, 850, 950];
        let right: Vec<i64> = (0..100).map(|i| i * 10).collect(); // 0, 10, 20, ..., 990

        match gpu_asof_join(&left, &right, AsofDirection::Backward) {
            Some(result) => {
                let expected = cpu_asof_backward(&left, &right);
                assert_eq!(
                    result, expected,
                    "GPU backward with large right should match CPU"
                );
            }
            None => {
                eprintln!("GPU not available, skipping test_gpu_asof_large_right_table");
            }
        }

        match gpu_asof_join(&left, &right, AsofDirection::Forward) {
            Some(result) => {
                let expected = cpu_asof_forward(&left, &right);
                assert_eq!(
                    result, expected,
                    "GPU forward with large right should match CPU"
                );
            }
            None => {}
        }

        match gpu_asof_join(&left, &right, AsofDirection::Nearest) {
            Some(result) => {
                let expected = cpu_asof_nearest(&left, &right);
                assert_eq!(
                    result, expected,
                    "GPU nearest with large right should match CPU"
                );
            }
            None => {}
        }
    }

    #[test]
    fn test_gpu_asof_single_element_right() {
        let left = vec![500i64, 1000, 1500];
        let right = vec![1000i64];

        if let Some(result) = gpu_asof_join(&left, &right, AsofDirection::Backward) {
            // 500 < 1000 -> None, 1000 <= 1000 -> Some(0), 1500 >= 1000 -> Some(0)
            assert_eq!(result, vec![None, Some(0), Some(0)]);
        }

        if let Some(result) = gpu_asof_join(&left, &right, AsofDirection::Forward) {
            // 500 -> 1000 (idx 0), 1000 -> 1000 (idx 0), 1500 -> None
            assert_eq!(result, vec![Some(0), Some(0), None]);
        }
    }

    #[test]
    fn test_gpu_asof_duplicate_times() {
        let left = vec![1000i64, 1000, 1000];
        let right = vec![998i64, 1000, 1000, 1002];

        if let Some(result) = gpu_asof_join(&left, &right, AsofDirection::Backward) {
            // All left rows are 1000. Backward finds largest right <= 1000.
            // partition_point(t <= 1000) = 3 (indices 0,1,2 are <=1000), so match = idx 2
            assert_eq!(result, vec![Some(2), Some(2), Some(2)]);
        }

        if let Some(result) = gpu_asof_join(&left, &right, AsofDirection::Forward) {
            // All left rows are 1000. Forward finds smallest right >= 1000.
            // partition_point(t < 1000) = 1 (only index 0 is <1000), so match = idx 1
            assert_eq!(result, vec![Some(1), Some(1), Some(1)]);
        }
    }
}
