//! wgpu device and queue initialization.
//!
//! Provides a global `WgpuContext` singleton (similar to the `RUNTIME` pattern in engine.rs).
//! Initialization is lazy — the GPU adapter is only requested on first access.
//! Returns `None` if:
//! - `LTSEQ_DISABLE_GPU` environment variable is set
//! - No suitable GPU adapter is found
//! - Device request fails
//!
//! ## GPU device selection
//!
//! Set `LTSEQ_GPU_DEVICE` before first use to pick a specific adapter:
//! - A pure integer string (e.g. `"0"`, `"1"`) selects by index in the enumerated adapter
//!   list, provided the index is within range.
//! - Any other string (including integers that are out of range) is treated as a
//!   case-insensitive substring match against the adapter name. The first matching
//!   adapter is used. For example, `"3070"` matches "NVIDIA GeForce RTX 3070".
//! - If unset, the adapter with the highest performance (discrete GPU preferred) is chosen
//!   via `PowerPreference::HighPerformance`, which is wgpu's standard heuristic.

use std::sync::LazyLock;

use super::buffer_pool::BufferPool;

/// Metadata about a GPU adapter, used for enumeration and info queries.
#[derive(Debug, Clone)]
pub struct GpuAdapterInfo {
    pub index: usize,
    pub name: String,
    pub backend: String,
    pub device_type: String,
}

/// Global GPU context — lazily initialized, `None` if GPU unavailable.
pub static GPU_CONTEXT: LazyLock<Option<WgpuContext>> = LazyLock::new(|| {
    if std::env::var("LTSEQ_DISABLE_GPU").is_ok() {
        return None;
    }
    WgpuContext::try_init()
});

/// Holds the wgpu adapter, device, and queue for GPU compute operations.
pub struct WgpuContext {
    #[allow(dead_code)]
    pub adapter: wgpu::Adapter,
    pub device: wgpu::Device,
    pub queue: wgpu::Queue,
    /// Whether the GPU shares physical memory with the CPU (Unified Memory Architecture).
    /// True for integrated GPUs (Apple Silicon, AMD APUs, Intel iGPUs, ARM SoCs like RK3588).
    /// On UMA systems, buffer "uploads" and "readbacks" don't cross a PCIe bus, so
    /// mapped_at_creation and direct MAP_READ can avoid driver-side copies.
    pub is_uma: bool,
    /// The wgpu device type for diagnostics / logging.
    #[allow(dead_code)]
    pub device_type: wgpu::DeviceType,
    /// Shared buffer pool for amortising allocation overhead across batches.
    pub buffer_pool: BufferPool,
    /// Adapter metadata (name, backend, device_type string) for Python-level queries.
    pub info: GpuAdapterInfo,
}

impl WgpuContext {
    /// Attempt to initialize wgpu, respecting `LTSEQ_GPU_DEVICE` if set.
    ///
    /// Returns `None` on failure (no adapter, device request rejected, etc.).
    fn try_init() -> Option<Self> {
        // Use pollster to block on the async wgpu initialization.
        // We can't use the existing RUNTIME because it may not be initialized yet
        // (LazyLock ordering), and wgpu init is a one-time cost.
        let instance = wgpu::Instance::new(&wgpu::InstanceDescriptor {
            backends: wgpu::Backends::all(),
            ..Default::default()
        });

        let adapter = Self::select_adapter(&instance)?;

        let raw_info = adapter.get_info();
        // Classify UMA: integrated GPUs share physical memory with the CPU, so there
        // is no PCIe bus to cross. On Metal (Apple Silicon) wgpu reports DeviceType::Other,
        // but all Apple GPUs are UMA. We treat that as UMA too.
        let is_uma = matches!(
            raw_info.device_type,
            wgpu::DeviceType::IntegratedGpu | wgpu::DeviceType::Other
        );

        let info = GpuAdapterInfo {
            index: 0, // selected adapter — index is informational only
            name: raw_info.name.clone(),
            backend: format!("{:?}", raw_info.backend),
            device_type: format!("{:?}", raw_info.device_type),
        };

        eprintln!(
            "[ltseq-gpu] adapter: {:?} | backend: {:?} | device_type: {:?} | UMA: {}",
            raw_info.name, raw_info.backend, raw_info.device_type, is_uma
        );

        // Request device with default limits (sufficient for compute shaders)
        let (device, queue) = pollster::block_on(adapter.request_device(
            &wgpu::DeviceDescriptor {
                label: Some("ltseq-gpu"),
                required_features: wgpu::Features::empty(),
                required_limits: wgpu::Limits::default(),
                memory_hints: wgpu::MemoryHints::Performance,
            },
            None,
        ))
        .ok()?;

        Some(WgpuContext {
            adapter,
            device,
            queue,
            is_uma,
            device_type: raw_info.device_type,
            buffer_pool: BufferPool::new(),
            info,
        })
    }

    /// Select an adapter based on `LTSEQ_GPU_DEVICE` env var, or fall back to
    /// wgpu's `HighPerformance` heuristic.
    ///
    /// Selection rules:
    /// - Pure integer within range → use as index
    /// - Anything else (including out-of-range integers) → case-insensitive name substring
    /// - No match → fall back to HighPerformance default
    fn select_adapter(instance: &wgpu::Instance) -> Option<wgpu::Adapter> {
        match std::env::var("LTSEQ_GPU_DEVICE") {
            Ok(selector) => {
                let adapters: Vec<wgpu::Adapter> =
                    instance.enumerate_adapters(wgpu::Backends::all());
                if adapters.is_empty() {
                    eprintln!("[ltseq-gpu] LTSEQ_GPU_DEVICE set but no adapters found");
                    return None;
                }

                // If the selector is a pure integer AND within the valid index range,
                // use it as an index. Otherwise treat it as a case-insensitive name
                // substring. This means "0", "1", "2" pick by index, while "3070" or
                // "radeon" match by name.
                if let Ok(idx) = selector.trim().parse::<usize>() {
                    if idx < adapters.len() {
                        let a = adapters.into_iter().nth(idx).unwrap();
                        let n = a.get_info().name.clone();
                        eprintln!("[ltseq-gpu] selected adapter by index {}: {:?}", idx, n);
                        return Some(a);
                    }
                    // Out-of-range integer → fall through to substring search
                    eprintln!(
                        "[ltseq-gpu] index {} out of range ({} adapters), trying name match",
                        idx,
                        adapters.len()
                    );
                }

                // Substring match (case-insensitive)
                let needle = selector.to_lowercase();
                for a in adapters {
                    let name = a.get_info().name.to_lowercase();
                    if name.contains(&needle) {
                        eprintln!(
                            "[ltseq-gpu] selected adapter by name match {:?}: {:?}",
                            selector,
                            a.get_info().name
                        );
                        return Some(a);
                    }
                }
                eprintln!(
                    "[ltseq-gpu] LTSEQ_GPU_DEVICE {:?} matched no adapter, falling back to default",
                    selector
                );
                // Fall back to HighPerformance default
                pollster::block_on(instance.request_adapter(&wgpu::RequestAdapterOptions {
                    power_preference: wgpu::PowerPreference::HighPerformance,
                    compatible_surface: None,
                    force_fallback_adapter: false,
                }))
            }
            Err(_) => {
                // No selection requested — use wgpu's HighPerformance heuristic
                pollster::block_on(instance.request_adapter(&wgpu::RequestAdapterOptions {
                    power_preference: wgpu::PowerPreference::HighPerformance,
                    compatible_surface: None,
                    force_fallback_adapter: false,
                }))
            }
        }
    }
}

/// Enumerate all available GPU adapters without touching the `GPU_CONTEXT` singleton.
///
/// Creates a temporary `wgpu::Instance` to enumerate adapters. Safe to call before
/// GPU init (or even when GPU is disabled). Returns a list of adapter metadata.
pub fn enumerate_gpus() -> Vec<GpuAdapterInfo> {
    let instance = wgpu::Instance::new(&wgpu::InstanceDescriptor {
        backends: wgpu::Backends::all(),
        ..Default::default()
    });
    instance
        .enumerate_adapters(wgpu::Backends::all())
        .into_iter()
        .enumerate()
        .map(|(i, a)| {
            let info = a.get_info();
            GpuAdapterInfo {
                index: i,
                name: info.name.clone(),
                backend: format!("{:?}", info.backend),
                device_type: format!("{:?}", info.device_type),
            }
        })
        .collect()
}

/// Return metadata about the currently selected GPU adapter, or `None` if unavailable.
pub fn selected_gpu_info() -> Option<(GpuAdapterInfo, bool)> {
    GPU_CONTEXT
        .as_ref()
        .map(|ctx| (ctx.info.clone(), ctx.is_uma))
}
