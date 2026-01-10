use std::process::Command;

fn main() {
    // Try to get Python info from the active venv
    // PyO3 will auto-detect from the Python interpreter

    // Get the venv Python executable
    let output = Command::new("python")
        .arg("-c")
        .arg("import sysconfig; print(sysconfig.get_config_var('LIBDIR'))")
        .output();

    if let Ok(out) = output {
        if out.status.success() {
            let lib_dir = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if !lib_dir.is_empty() {
                println!("cargo:rustc-link-search=native={}", lib_dir);
            }
        }
    }

    // Get Python version
    let version_output = Command::new("python")
        .arg("-c")
        .arg("import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
        .output();

    if let Ok(out) = version_output {
        if out.status.success() {
            let version = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if !version.is_empty() {
                println!("cargo:rustc-link-lib=dylib=python{}", version);
            }
        }
    }
}
