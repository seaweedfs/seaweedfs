use std::env;
use std::io;
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, ExitCode, ExitStatus};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::os::unix::process::CommandExt;

static TERMINATE: AtomicBool = AtomicBool::new(false);

#[cfg(unix)]
type SigHandler = extern "C" fn(i32);
#[cfg(unix)]
const SIGINT: i32 = 2;
#[cfg(unix)]
const SIGTERM: i32 = 15;

#[cfg(unix)]
extern "C" {
    fn signal(sig: i32, handler: SigHandler) -> SigHandler;
}

#[cfg(unix)]
extern "C" fn handle_termination_signal(_sig: i32) {
    TERMINATE.store(true, Ordering::SeqCst);
}

#[derive(Clone)]
struct FrontendPorts {
    bind_ip: String,
    http_port: u16,
    grpc_port: u16,
    public_port: u16,
}

#[derive(Clone)]
struct BackendPorts {
    bind_ip: String,
    http_port: u16,
    grpc_port: u16,
    public_port: u16,
}

#[derive(Clone)]
struct ProxySpec {
    frontend_addr: String,
    backend_addr: String,
}

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

    let forwarded = normalize_volume_args(args);
    let mode = env::var("VOLUME_SERVER_RUST_MODE")
        .unwrap_or_else(|_| "exec".to_string())
        .to_lowercase();

    match mode.as_str() {
        "exec" => run_exec_mode(&forwarded),
        "proxy" => run_proxy_mode(&forwarded),
        "native" => run_native_mode(&forwarded),
        other => Err(format!(
            "unsupported VOLUME_SERVER_RUST_MODE {:?} (supported: exec, proxy, native)",
            other
        )),
    }
}

fn normalize_volume_args(args: Vec<String>) -> Vec<String> {
    if args.iter().any(|a| a == "volume") {
        return args;
    }
    let mut forwarded = Vec::with_capacity(args.len() + 1);
    forwarded.push("volume".to_string());
    forwarded.extend(args);
    forwarded
}

fn run_exec_mode(forwarded: &[String]) -> Result<(), String> {
    let weed_binary = resolve_weed_binary()?;

    #[cfg(unix)]
    {
        let exec_err = Command::new(&weed_binary).args(forwarded).exec();
        return Err(format!(
            "exec {} failed: {}",
            weed_binary.display(),
            exec_err
        ));
    }

    #[cfg(not(unix))]
    {
        let status = Command::new(&weed_binary)
            .args(forwarded)
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

fn run_proxy_mode(forwarded: &[String]) -> Result<(), String> {
    install_signal_handlers();

    let weed_binary = resolve_weed_binary()?;
    let frontend = parse_frontend_ports(forwarded)?;

    let backend_bind_ip = "127.0.0.1";
    let backend =
        allocate_backend_ports(backend_bind_ip, frontend.public_port == frontend.http_port)?;

    let mut backend_args = replace_flag_value(forwarded, "-ip", &backend.bind_ip);
    backend_args = replace_flag_value(&backend_args, "-port", &backend.http_port.to_string());
    backend_args = replace_flag_value(&backend_args, "-port.grpc", &backend.grpc_port.to_string());
    backend_args = replace_flag_value(
        &backend_args,
        "-port.public",
        &backend.public_port.to_string(),
    );

    let mut child = spawn_backend(&weed_binary, &backend_args)?;

    let mut handles = Vec::new();
    for spec in build_listener_specs(&frontend, &backend) {
        handles.push(start_proxy_listener(spec));
    }

    let mut terminated_by_signal = false;
    let child_status: ExitStatus = loop {
        if TERMINATE.load(Ordering::SeqCst) {
            terminated_by_signal = true;
            terminate_child(&mut child);
            break wait_child(&mut child)?;
        }

        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => thread::sleep(Duration::from_millis(100)),
            Err(err) => return Err(format!("failed to poll backend process: {}", err)),
        }
    };

    TERMINATE.store(true, Ordering::SeqCst);
    for handle in handles {
        let _ = handle.join();
    }

    if terminated_by_signal {
        return Ok(());
    }
    if child_status.success() {
        Ok(())
    } else {
        Err(format!(
            "backend volume process exited with status {}",
            child_status
        ))
    }
}

fn run_native_mode(forwarded: &[String]) -> Result<(), String> {
    eprintln!(
        "weed-volume-rs: native mode bootstrap active; delegating to Go backend while Rust handlers are implemented"
    );
    run_proxy_mode(forwarded)
}

fn spawn_backend(weed_binary: &PathBuf, backend_args: &[String]) -> Result<Child, String> {
    Command::new(weed_binary)
        .args(backend_args)
        .spawn()
        .map_err(|e| format!("spawn backend {} failed: {}", weed_binary.display(), e))
}

fn wait_child(child: &mut Child) -> Result<ExitStatus, String> {
    child
        .wait()
        .map_err(|e| format!("wait backend process failed: {}", e))
}

fn terminate_child(child: &mut Child) {
    let _ = child.kill();
}

fn start_proxy_listener(spec: ProxySpec) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let listener = match TcpListener::bind(&spec.frontend_addr) {
            Ok(v) => v,
            Err(err) => {
                eprintln!(
                    "weed-volume-rs: failed to bind proxy listener {}: {}",
                    spec.frontend_addr, err
                );
                TERMINATE.store(true, Ordering::SeqCst);
                return;
            }
        };
        if let Err(err) = listener.set_nonblocking(true) {
            eprintln!(
                "weed-volume-rs: failed to set nonblocking listener {}: {}",
                spec.frontend_addr, err
            );
            TERMINATE.store(true, Ordering::SeqCst);
            return;
        }

        while !TERMINATE.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((stream, _)) => {
                    let backend_addr = spec.backend_addr.clone();
                    thread::spawn(move || {
                        if let Err(err) = proxy_connection(stream, &backend_addr) {
                            eprintln!(
                                "weed-volume-rs: proxy connection to {} failed: {}",
                                backend_addr, err
                            );
                        }
                    });
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(20));
                }
                Err(err) => {
                    eprintln!(
                        "weed-volume-rs: accept failed on {}: {}",
                        spec.frontend_addr, err
                    );
                    thread::sleep(Duration::from_millis(100));
                }
            }
        }
    })
}

fn proxy_connection(mut downstream: TcpStream, backend_addr: &str) -> io::Result<()> {
    let mut upstream = TcpStream::connect(backend_addr)?;

    let _ = downstream.set_nodelay(true);
    let _ = upstream.set_nodelay(true);

    let mut downstream_read = downstream.try_clone()?;
    let mut upstream_write = upstream.try_clone()?;
    let upload = thread::spawn(move || {
        let _ = io::copy(&mut downstream_read, &mut upstream_write);
        let _ = upstream_write.shutdown(Shutdown::Write);
    });

    let _ = io::copy(&mut upstream, &mut downstream);
    let _ = downstream.shutdown(Shutdown::Write);
    let _ = upstream.shutdown(Shutdown::Read);

    let _ = upload.join();
    Ok(())
}

fn build_listener_specs(frontend: &FrontendPorts, backend: &BackendPorts) -> Vec<ProxySpec> {
    let mut specs = Vec::new();
    specs.push(ProxySpec {
        frontend_addr: format!("{}:{}", frontend.bind_ip, frontend.http_port),
        backend_addr: format!("{}:{}", backend.bind_ip, backend.http_port),
    });
    specs.push(ProxySpec {
        frontend_addr: format!("{}:{}", frontend.bind_ip, frontend.grpc_port),
        backend_addr: format!("{}:{}", backend.bind_ip, backend.grpc_port),
    });
    if frontend.public_port != frontend.http_port {
        specs.push(ProxySpec {
            frontend_addr: format!("{}:{}", frontend.bind_ip, frontend.public_port),
            backend_addr: format!("{}:{}", backend.bind_ip, backend.public_port),
        });
    }
    specs
}

fn parse_frontend_ports(args: &[String]) -> Result<FrontendPorts, String> {
    let bind_ip = extract_flag(args, "-ip").unwrap_or_else(|| "127.0.0.1".to_string());
    let http_port = parse_port(
        &extract_flag(args, "-port").ok_or_else(|| "missing -port value".to_string())?,
        "-port",
    )?;
    let grpc_port = parse_port(
        &extract_flag(args, "-port.grpc").ok_or_else(|| "missing -port.grpc value".to_string())?,
        "-port.grpc",
    )?;
    let public_port = parse_port(
        &extract_flag(args, "-port.public").unwrap_or_else(|| http_port.to_string()),
        "-port.public",
    )?;

    Ok(FrontendPorts {
        bind_ip,
        http_port,
        grpc_port,
        public_port,
    })
}

fn allocate_backend_ports(bind_ip: &str, share_public_http: bool) -> Result<BackendPorts, String> {
    let http_port = allocate_free_port(bind_ip)?;
    let grpc_port = allocate_free_port(bind_ip)?;
    let public_port = if share_public_http {
        http_port
    } else {
        allocate_free_port(bind_ip)?
    };
    Ok(BackendPorts {
        bind_ip: bind_ip.to_string(),
        http_port,
        grpc_port,
        public_port,
    })
}

fn allocate_free_port(bind_ip: &str) -> Result<u16, String> {
    let listener = TcpListener::bind((bind_ip, 0))
        .or_else(|_| TcpListener::bind(("127.0.0.1", 0)))
        .map_err(|e| format!("allocate free port failed: {}", e))?;
    let port = listener
        .local_addr()
        .map_err(|e| format!("read allocated port failed: {}", e))?
        .port();
    drop(listener);
    Ok(port)
}

fn extract_flag(args: &[String], key: &str) -> Option<String> {
    let with_equals = format!("{}=", key);
    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if let Some(v) = arg.strip_prefix(&with_equals) {
            return Some(v.to_string());
        }
        if arg == key {
            if i + 1 < args.len() {
                return Some(args[i + 1].clone());
            }
            return None;
        }
        i += 1;
    }
    None
}

fn replace_flag_value(args: &[String], key: &str, value: &str) -> Vec<String> {
    let with_equals = format!("{}=", key);
    let mut out = Vec::with_capacity(args.len() + 2);
    let mut i = 0;
    let mut replaced = false;
    while i < args.len() {
        let arg = &args[i];
        if arg.starts_with(&with_equals) {
            out.push(format!("{}={}", key, value));
            replaced = true;
            i += 1;
            continue;
        }
        if arg == key {
            out.push(arg.clone());
            out.push(value.to_string());
            replaced = true;
            i += 2;
            continue;
        }
        out.push(arg.clone());
        i += 1;
    }
    if !replaced {
        out.push(format!("{}={}", key, value));
    }
    out
}

fn parse_port(value: &str, flag: &str) -> Result<u16, String> {
    value
        .parse::<u16>()
        .map_err(|e| format!("invalid {} value {:?}: {}", flag, value, e))
}

fn install_signal_handlers() {
    #[cfg(unix)]
    unsafe {
        let _ = signal(SIGINT, handle_termination_signal);
        let _ = signal(SIGTERM, handle_termination_signal);
    }
}

fn print_help() {
    println!("weed-volume-rs");
    println!();
    println!("Rust compatibility launcher for SeaweedFS volume server.");
    println!("Modes:");
    println!("  - exec  (default): exec Go weed volume process directly");
    println!("  - proxy: run Rust TCP proxy front-end and supervise Go weed backend");
    println!("  - native: bootstrap native mode entrypoint (currently supervises/proxies Go backend)");
    println!();
    println!("Environment:");
    println!("  VOLUME_SERVER_RUST_MODE=exec|proxy|native");
    println!("  WEED_BINARY=/path/to/weed");
    println!();
    println!("Examples:");
    println!("  weed-volume-rs -config_dir=/tmp/cfg volume -ip=127.0.0.1 -port=8080 ...");
    println!("  VOLUME_SERVER_RUST_MODE=proxy weed-volume-rs -config_dir=/tmp/cfg volume -ip=127.0.0.1 -port=8080 ...");
    println!("  VOLUME_SERVER_RUST_MODE=native weed-volume-rs -config_dir=/tmp/cfg volume -ip=127.0.0.1 -port=8080 ...");
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
