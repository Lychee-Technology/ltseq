/// Build script for ltseq_core.
///
/// When the `gpu` feature is enabled, this script compiles and links the
/// C++ cuDF engine.  Two modes are supported:
///
/// 1. **Stub mode** (default, no RAPIDS needed):
///    Compiles `cpp/src/cudf_engine_stub.c` into a static library that
///    returns `false`/`-ENOSYS` for every call.  This allows the crate to
///    compile and run on systems without RAPIDS (e.g. CUDA 13.1 before
///    RAPIDS 25.06).
///
/// 2. **Real engine mode** (set `CUDF_ENGINE_LIB`):
///    Links against a pre-built `libcudf_engine.so` produced by the CMake
///    build in `cpp/`.  No stub is compiled.
///
///    ```
///    CUDF_ENGINE_LIB=/path/to/dir  cargo build --features gpu
///    ```
///    The directory must contain `libcudf_engine.so`.
///
/// The `cc` build-dependency is always compiled; it does nothing unless the
/// `gpu` feature is active.

fn main() {
    // Only hook up the C library when the gpu feature is enabled.
    if std::env::var("CARGO_FEATURE_GPU").is_err() {
        return;
    }

    if let Ok(lib_dir) = std::env::var("CUDF_ENGINE_LIB") {
        // ── Real engine mode ──────────────────────────────────────────────
        // Link against the externally-built libcudf_engine.so.
        println!("cargo:rustc-link-search=native={lib_dir}");
        println!("cargo:rustc-link-lib=dylib=cudf_engine");
        // Bake the engine dir and RAPIDS lib dirs into rpath so the .so
        // can be loaded without LD_LIBRARY_PATH.
        println!("cargo:rustc-link-arg=-Wl,-rpath,{lib_dir}");
        // Also add RAPIDS transitive deps if CUDF_ENGINE_RAPIDS_LIB is set.
        if let Ok(rapids_lib) = std::env::var("CUDF_ENGINE_RAPIDS_LIB") {
            for dir in rapids_lib.split(':') {
                println!("cargo:rustc-link-arg=-Wl,-rpath,{dir}");
            }
        }
        // Re-run if the library directory changes.
        println!("cargo:rerun-if-env-changed=CUDF_ENGINE_LIB");
        println!("cargo:rerun-if-env-changed=CUDF_ENGINE_RAPIDS_LIB");
    } else {
        // ── Stub mode ─────────────────────────────────────────────────────
        // Compile the no-op stub and link it as a static library.
        cc::Build::new()
            .file("cpp/src/cudf_engine_stub.c")
            .include("cpp/include")
            // Treat warnings as errors to catch ABI drift early.
            .warnings_into_errors(true)
            .compile("cudf_engine");

        // Re-run if the stub or header change.
        println!("cargo:rerun-if-changed=cpp/src/cudf_engine_stub.c");
        println!("cargo:rerun-if-changed=cpp/include/cudf_engine.h");
    }
}
