use std::env;
use std::ffi::OsString;
use std::path::PathBuf;
use std::process::{Command, ExitCode};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::os::unix::process::CommandExt;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("weed-volume-rs: {err}");
            ExitCode::from(1)
        }
    }
}

fn run() -> Result<(), String> {
    let args: Vec<String> = env::args().skip(1).collect();

    if args.iter().any(|a| a == "-h" || a == "--help") {
        print_help();
        return Ok(());
    }
    if args.iter().any(|a| a == "--version") {
        println!("weed-volume-rs 0.1.0");
        return Ok(());
    }

    let mut forwarded = Vec::<String>::new();
    let has_volume_subcommand = args.iter().any(|a| a == "volume");
    if has_volume_subcommand {
        forwarded.extend(args);
    } else {
        forwarded.push("volume".to_string());
        forwarded.extend(args);
    }

    let weed_binary = resolve_weed_binary()?;

    #[cfg(unix)]
    {
        let exec_err = Command::new(&weed_binary).args(&forwarded).exec();
        return Err(format!(
            "exec {} failed: {}",
            weed_binary.display(),
            exec_err
        ));
    }

    #[cfg(not(unix))]
    {
        let status = Command::new(&weed_binary)
            .args(&forwarded)
            .status()
            .map_err(|e| format!("spawn {} failed: {}", weed_binary.display(), e))?;
        if status.success() {
            return Ok(());
        }
        return Err(format!(
            "delegated process {} exited with status {}",
            weed_binary.display(),
            status
        ));
    }
}

fn print_help() {
    println!("weed-volume-rs");
    println!();
    println!("Rust compatibility launcher for SeaweedFS volume server.");
    println!("It forwards all volume-server flags to the Go weed binary.");
    println!();
    println!("Examples:");
    println!("  weed-volume-rs -ip=127.0.0.1 -port=8080 -master=127.0.0.1:9333 ...");
    println!("  weed-volume-rs volume -ip=127.0.0.1 -port=8080 -master=127.0.0.1:9333 ...");
}

fn resolve_weed_binary() -> Result<PathBuf, String> {
    if let Some(from_env) = env::var_os("WEED_BINARY") {
        let path = PathBuf::from(from_env);
        if is_executable_file(&path) {
            return Ok(path);
        }
        return Err(format!(
            "WEED_BINARY is set but not executable: {}",
            path.display()
        ));
    }

    let repo_root = resolve_repo_root()?;
    let local_weed = repo_root.join("weed").join("weed");
    if is_executable_file(&local_weed) {
        return Ok(local_weed);
    }

    let bin_dir = env::temp_dir().join("seaweedfs_volume_server_it_bin");
    std::fs::create_dir_all(&bin_dir)
        .map_err(|e| format!("create binary directory {}: {}", bin_dir.display(), e))?;
    let bin_path = bin_dir.join("weed");
    if is_executable_file(&bin_path) {
        return Ok(bin_path);
    }

    build_weed_binary(&repo_root, &bin_path)?;
    if !is_executable_file(&bin_path) {
        return Err(format!(
            "built weed binary is not executable: {}",
            bin_path.display()
        ));
    }
    Ok(bin_path)
}

fn resolve_repo_root() -> Result<PathBuf, String> {
    if let Some(from_env) = env::var_os("SEAWEEDFS_REPO_ROOT") {
        return Ok(PathBuf::from(from_env));
    }
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .ok_or_else(|| "unable to detect repository root from CARGO_MANIFEST_DIR".to_string())?;
    Ok(repo_root.to_path_buf())
}

fn build_weed_binary(repo_root: &PathBuf, output_path: &PathBuf) -> Result<(), String> {
    let mut cmd = Command::new("go");
    cmd.arg("build").arg("-o").arg(output_path).arg(".");
    cmd.current_dir(repo_root.join("weed"));

    let output = cmd
        .output()
        .map_err(|e| format!("failed to execute go build: {}", e))?;
    if output.status.success() {
        return Ok(());
    }

    let mut msg = String::new();
    msg.push_str("go build failed");
    if !output.stdout.is_empty() {
        msg.push_str("\nstdout:\n");
        msg.push_str(&String::from_utf8_lossy(&output.stdout));
    }
    if !output.stderr.is_empty() {
        msg.push_str("\nstderr:\n");
        msg.push_str(&String::from_utf8_lossy(&output.stderr));
    }
    Err(msg)
}

fn is_executable_file(path: &PathBuf) -> bool {
    let metadata = match std::fs::metadata(path) {
        Ok(v) => v,
        Err(_) => return false,
    };
    if !metadata.is_file() {
        return false;
    }

    #[cfg(unix)]
    {
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

#[allow(dead_code)]
fn _collect_os_args() -> Vec<OsString> {
    env::args_os().collect()
}
