//! GPU buffer pool for amortizing wgpu buffer allocation overhead.
//!
//! # Problem
//! Every batch dispatch currently calls `create_buffer_init` / `create_buffer`,
//! which involves driver allocation on every invocation.  For 100M-row input at
//! 16 384 rows per batch that is ~6 100 allocation calls per column.
//!
//! # Design
//! A size-class free-list pool.  Buffers are bucketed by their *rounded* size
//! (next power-of-two, minimum 4 KiB) and their `BufferUsages` flags.  When
//! a caller requests a buffer it receives either a recycled one from the free
//! list or a freshly allocated one.  When the caller is done, it returns the
//! buffer via `release()`.
//!
//! The pool is thread-safe via an internal `Mutex`.  Callers are responsible
//! for zero-initialising returned buffers if required (the pool does not clear
//! contents on release).
//!
//! # Usage
//! ```rust,ignore
//! // Acquire a staging buffer of at least `needed` bytes
//! let buf = ctx.buffer_pool.acquire(&ctx.device, needed, STAGING_USAGE);
//! // ... use buf ...
//! ctx.buffer_pool.release(buf);
//! ```

use std::collections::HashMap;
use std::sync::Mutex;

/// Minimum size class: 4 KiB.
const MIN_SIZE_CLASS: u64 = 4096;

/// Maximum number of idle buffers per (size_class, usage) bucket.
/// Beyond this the excess buffers are dropped (GPU memory freed).
const MAX_PER_BUCKET: usize = 8;

/// Key for the free-list map.
#[derive(Hash, PartialEq, Eq, Clone)]
struct BucketKey {
    /// Size class (power-of-two bytes).
    size_class: u64,
    /// Buffer usage flags (determines memory heap on Vulkan/Metal).
    usage: wgpu::BufferUsages,
}

pub struct BufferPool {
    free_lists: Mutex<HashMap<BucketKey, Vec<wgpu::Buffer>>>,
}

impl std::fmt::Debug for BufferPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let lists = self.free_lists.lock().unwrap();
        let total: usize = lists.values().map(|v| v.len()).sum();
        write!(f, "BufferPool {{ idle_buffers: {total} }}")
    }
}

impl BufferPool {
    pub fn new() -> Self {
        Self {
            free_lists: Mutex::new(HashMap::new()),
        }
    }

    /// Compute the size class for a requested byte count.
    fn size_class(needed: u64) -> u64 {
        needed.next_power_of_two().max(MIN_SIZE_CLASS)
    }

    /// Acquire a buffer of at least `needed` bytes with the given `usage`.
    ///
    /// Returns a recycled buffer from the pool if one is available, otherwise
    /// allocates a new one.  The returned buffer may be larger than `needed`
    /// (up to the next power-of-two).
    pub fn acquire(
        &self,
        device: &wgpu::Device,
        needed: u64,
        usage: wgpu::BufferUsages,
    ) -> wgpu::Buffer {
        let sc = Self::size_class(needed);
        let key = BucketKey {
            size_class: sc,
            usage,
        };
        // Try free list first
        if let Some(buf) = self
            .free_lists
            .lock()
            .unwrap()
            .get_mut(&key)
            .and_then(|v| v.pop())
        {
            return buf;
        }

        // Allocate fresh
        device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("pooled"),
            size: sc,
            usage,
            mapped_at_creation: false,
        })
    }

    /// Return a buffer to the pool for future reuse.
    ///
    /// The buffer must not be mapped when returned.  If the bucket is full
    /// (exceeds `MAX_PER_BUCKET`) the buffer is silently dropped (GPU memory
    /// is freed at that point).
    pub fn release(&self, buf: wgpu::Buffer) {
        // Recover the size from the buffer itself
        let sc = Self::size_class(buf.size());
        let usage = buf.usage();
        let key = BucketKey {
            size_class: sc,
            usage,
        };
        let mut lists = self.free_lists.lock().unwrap();
        let bucket = lists.entry(key).or_default();
        if bucket.len() < MAX_PER_BUCKET {
            bucket.push(buf);
        }
        // If over limit, `buf` is dropped here → GPU memory freed
    }

    /// Number of idle buffers currently held in the pool (for diagnostics).
    #[allow(dead_code)]
    pub fn idle_count(&self) -> usize {
        self.free_lists
            .lock()
            .unwrap()
            .values()
            .map(|v| v.len())
            .sum()
    }
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new()
    }
}
