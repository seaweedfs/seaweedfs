use std::env;
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, ExitCode, ExitStatus};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
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
    role: ListenerRole,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ListenerRole {
    HttpAdmin,
    Grpc,
    HttpPublic,
}

#[derive(Clone, Default)]
struct NativeHttpConfig {
    jwt_signing_enabled: bool,
    access_ui_enabled: bool,
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
        .unwrap_or_else(|_| "native".to_string())
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
    run_supervised_mode(forwarded, false)
}

fn run_supervised_mode(
    forwarded: &[String],
    enable_native_http_admin_control: bool,
) -> Result<(), String> {
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

    let native_http_config = if enable_native_http_admin_control {
        Some(Arc::new(load_native_http_config(forwarded)))
    } else {
        None
    };

    let mut handles = Vec::new();
    for spec in build_listener_specs(&frontend, &backend) {
        let native_http_role = if enable_native_http_admin_control {
            match spec.role {
                ListenerRole::HttpAdmin => Some(ListenerRole::HttpAdmin),
                ListenerRole::HttpPublic => Some(ListenerRole::HttpPublic),
                ListenerRole::Grpc => None,
            }
        } else {
            None
        };
        handles.push(start_proxy_listener(
            spec,
            native_http_role,
            native_http_config.clone(),
        ));
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
        "weed-volume-rs: native mode bootstrap active; serving Rust control/static/UI paths (/status, /healthz, OPTIONS, /ui/index.html, /favicon.ico, /seaweedfsstatic/*), delegating remaining handlers to Go backend"
    );
    run_supervised_mode(forwarded, true)
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

fn start_proxy_listener(
    spec: ProxySpec,
    native_http_role: Option<ListenerRole>,
    native_http_config: Option<Arc<NativeHttpConfig>>,
) -> thread::JoinHandle<()> {
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
                    let native_http_role = native_http_role;
                    let native_http_config = native_http_config.clone();
                    thread::spawn(move || {
                        let mut stream = stream;
                        if let Some(role) = native_http_role {
                            match try_handle_native_http(
                                &mut stream,
                                role,
                                native_http_config.as_deref(),
                            ) {
                                Ok(true) => return,
                                Ok(false) => {}
                                Err(err) => {
                                    eprintln!(
                                        "weed-volume-rs: native admin HTTP handler failed, falling back to backend proxy: {}",
                                        err
                                    );
                                }
                            }
                        }

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
        role: ListenerRole::HttpAdmin,
    });
    specs.push(ProxySpec {
        frontend_addr: format!("{}:{}", frontend.bind_ip, frontend.grpc_port),
        backend_addr: format!("{}:{}", backend.bind_ip, backend.grpc_port),
        role: ListenerRole::Grpc,
    });
    if frontend.public_port != frontend.http_port {
        specs.push(ProxySpec {
            frontend_addr: format!("{}:{}", frontend.bind_ip, frontend.public_port),
            backend_addr: format!("{}:{}", backend.bind_ip, backend.public_port),
            role: ListenerRole::HttpPublic,
        });
    }
    specs
}

fn try_handle_native_http(
    stream: &mut TcpStream,
    role: ListenerRole,
    config: Option<&NativeHttpConfig>,
) -> io::Result<bool> {
    let parsed = match peek_http_request_headers(stream)? {
        Some(v) => v,
        None => return Ok(false),
    };

    if role == ListenerRole::HttpAdmin && !is_admin_method_supported(&parsed.method) {
        consume_bytes(stream, parsed.header_len + parsed.content_length)?;
        write_native_http_response(
            stream,
            "400 Bad Request",
            "text/plain; charset=utf-8",
            b"unsupported method\n",
            false,
            parsed.request_id.as_deref(),
        )?;
        let _ = stream.shutdown(Shutdown::Both);
        return Ok(true);
    }

    if role == ListenerRole::HttpPublic && !is_public_read_method(&parsed.method) {
        consume_bytes(stream, parsed.header_len + parsed.content_length)?;
        write_native_public_noop_response(
            stream,
            parsed.origin.is_some(),
            parsed.request_id.as_deref(),
        )?;
        let _ = stream.shutdown(Shutdown::Both);
        return Ok(true);
    }

    if parsed.method == "OPTIONS" {
        consume_bytes(stream, parsed.header_len)?;
        write_native_options_response(stream, role, parsed.origin.is_some())?;
        let _ = stream.shutdown(Shutdown::Both);
        return Ok(true);
    }

    let is_head_or_get = parsed.method == "GET" || parsed.method == "HEAD";
    if is_head_or_get && parsed.path == "/favicon.ico" {
        consume_bytes(stream, parsed.header_len)?;
        write_native_http_response(
            stream,
            "200 OK",
            "image/x-icon",
            b"",
            parsed.method == "HEAD",
            parsed.request_id.as_deref(),
        )?;
        let _ = stream.shutdown(Shutdown::Both);
        return Ok(true);
    }

    if is_head_or_get && parsed.path.starts_with("/seaweedfsstatic/") {
        consume_bytes(stream, parsed.header_len)?;
        write_native_http_response(
            stream,
            "200 OK",
            "application/octet-stream",
            b"",
            parsed.method == "HEAD",
            parsed.request_id.as_deref(),
        )?;
        let _ = stream.shutdown(Shutdown::Both);
        return Ok(true);
    }

    if role == ListenerRole::HttpAdmin && is_head_or_get && parsed.path == "/ui/index.html" {
        consume_bytes(stream, parsed.header_len)?;
        if is_ui_access_denied(config) {
            write_native_http_response(
                stream,
                "401 Unauthorized",
                "text/plain; charset=utf-8",
                b"ui access denied\n",
                parsed.method == "HEAD",
                parsed.request_id.as_deref(),
            )?;
        } else {
            write_native_http_response(
                stream,
                "200 OK",
                "text/html; charset=utf-8",
                native_ui_index_html().as_bytes(),
                parsed.method == "HEAD",
                parsed.request_id.as_deref(),
            )?;
        }
        let _ = stream.shutdown(Shutdown::Both);
        return Ok(true);
    }

    let is_admin_control = role == ListenerRole::HttpAdmin
        && (parsed.path == "/status" || parsed.path == "/healthz")
        && (parsed.method == "GET" || parsed.method == "HEAD");
    if !is_admin_control {
        return Ok(false);
    }

    consume_bytes(stream, parsed.header_len)?;

    if parsed.path == "/status" {
        write_native_status_response(stream, &parsed.method, parsed.request_id.as_deref())?;
    } else {
        write_native_healthz_response(stream, &parsed.method, parsed.request_id.as_deref())?;
    }

    let _ = stream.shutdown(Shutdown::Both);
    Ok(true)
}

struct ParsedHttpRequest {
    method: String,
    path: String,
    request_id: Option<String>,
    origin: Option<String>,
    content_length: usize,
    header_len: usize,
}

fn peek_http_request_headers(stream: &TcpStream) -> io::Result<Option<ParsedHttpRequest>> {
    const MAX_HEADER_BYTES: usize = 8192;
    const MAX_PEEK_ATTEMPTS: usize = 25;

    let mut buf = [0u8; MAX_HEADER_BYTES];
    for _ in 0..MAX_PEEK_ATTEMPTS {
        let n = stream.peek(&mut buf)?;
        if n == 0 {
            return Ok(None);
        }
        let data = &buf[..n];
        if let Some(end) = find_header_terminator(data) {
            return Ok(parse_http_request_headers(data, end + 4));
        }
        if n == MAX_HEADER_BYTES {
            return Ok(None);
        }
        thread::sleep(Duration::from_millis(4));
    }
    Ok(None)
}

fn find_header_terminator(data: &[u8]) -> Option<usize> {
    if data.len() < 4 {
        return None;
    }
    for i in 0..=(data.len() - 4) {
        if data[i..i + 4] == *b"\r\n\r\n" {
            return Some(i);
        }
    }
    None
}

fn parse_http_request_headers(data: &[u8], header_len: usize) -> Option<ParsedHttpRequest> {
    let header_text = String::from_utf8_lossy(&data[..header_len]);
    let mut lines = header_text.split("\r\n");
    let request_line = lines.next()?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next()?.to_string();
    let raw_path = request_parts.next()?.to_string();

    let path = raw_path
        .split_once('?')
        .map(|(p, _)| p.to_string())
        .unwrap_or(raw_path);

    let mut request_id = None;
    let mut origin = None;
    let mut content_length = 0usize;
    for line in lines {
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("x-amz-request-id") {
                request_id = Some(value.trim().to_string());
            } else if name.eq_ignore_ascii_case("origin") {
                origin = Some(value.trim().to_string());
            } else if name.eq_ignore_ascii_case("content-length") {
                if let Ok(v) = value.trim().parse::<usize>() {
                    content_length = v;
                }
            }
        }
    }

    Some(ParsedHttpRequest {
        method,
        path,
        request_id,
        origin,
        content_length,
        header_len,
    })
}

fn is_admin_method_supported(method: &str) -> bool {
    matches!(
        method,
        "GET" | "HEAD" | "POST" | "PUT" | "DELETE" | "OPTIONS"
    )
}

fn is_public_read_method(method: &str) -> bool {
    matches!(method, "GET" | "HEAD" | "OPTIONS")
}

fn consume_bytes(stream: &mut TcpStream, mut remaining: usize) -> io::Result<()> {
    let mut discard = [0u8; 1024];
    while remaining > 0 {
        let to_read = remaining.min(discard.len());
        let n = stream.read(&mut discard[..to_read])?;
        if n == 0 {
            break;
        }
        remaining -= n;
    }
    Ok(())
}

fn write_native_status_response(
    stream: &mut TcpStream,
    method: &str,
    request_id: Option<&str>,
) -> io::Result<()> {
    let body = br#"{"Version":"rust-native-bootstrap","DiskStatuses":[],"Volumes":[]}"#;
    write_native_http_response(
        stream,
        "200 OK",
        "application/json",
        body,
        method == "HEAD",
        request_id,
    )
}

fn write_native_healthz_response(
    stream: &mut TcpStream,
    method: &str,
    request_id: Option<&str>,
) -> io::Result<()> {
    let body = b"ok\n";
    write_native_http_response(
        stream,
        "200 OK",
        "text/plain; charset=utf-8",
        body,
        method == "HEAD",
        request_id,
    )
}

fn write_native_options_response(
    stream: &mut TcpStream,
    role: ListenerRole,
    include_cors_headers: bool,
) -> io::Result<()> {
    let allow_methods = if role == ListenerRole::HttpPublic {
        "GET, OPTIONS"
    } else {
        "PUT, POST, GET, DELETE, OPTIONS"
    };

    let mut response = String::new();
    response.push_str("HTTP/1.1 200 OK\r\n");
    response.push_str("Server: SeaweedFS Volume (rust-native-bootstrap)\r\n");
    response.push_str("Connection: close\r\n");
    response.push_str("Access-Control-Allow-Methods: ");
    response.push_str(allow_methods);
    response.push_str("\r\n");
    response.push_str("Access-Control-Allow-Headers: *\r\n");
    if include_cors_headers {
        response.push_str("Access-Control-Allow-Origin: *\r\n");
        response.push_str("Access-Control-Allow-Credentials: true\r\n");
    }
    response.push_str("Content-Length: 0\r\n");
    response.push_str("\r\n");

    stream.write_all(response.as_bytes())?;
    stream.flush()?;
    Ok(())
}

fn write_native_public_noop_response(
    stream: &mut TcpStream,
    include_cors_headers: bool,
    request_id: Option<&str>,
) -> io::Result<()> {
    let mut response = String::new();
    response.push_str("HTTP/1.1 200 OK\r\n");
    response.push_str("Server: SeaweedFS Volume (rust-native-bootstrap)\r\n");
    response.push_str("Connection: close\r\n");
    response.push_str("Content-Length: 0\r\n");
    if let Some(request_id_value) = request_id {
        response.push_str("x-amz-request-id: ");
        response.push_str(request_id_value);
        response.push_str("\r\n");
    }
    if include_cors_headers {
        response.push_str("Access-Control-Allow-Origin: *\r\n");
        response.push_str("Access-Control-Allow-Credentials: true\r\n");
    }
    response.push_str("\r\n");

    stream.write_all(response.as_bytes())?;
    stream.flush()?;
    Ok(())
}

fn write_native_http_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &[u8],
    omit_body: bool,
    request_id: Option<&str>,
) -> io::Result<()> {
    let mut response = format!(
        "HTTP/1.1 {}\r\nServer: SeaweedFS Volume (rust-native-bootstrap)\r\nConnection: close\r\nContent-Type: {}\r\nContent-Length: {}\r\n",
        status,
        content_type,
        body.len()
    );
    if let Some(request_id_value) = request_id {
        response.push_str("x-amz-request-id: ");
        response.push_str(request_id_value);
        response.push_str("\r\n");
    }
    response.push_str("\r\n");

    stream.write_all(response.as_bytes())?;
    if !omit_body {
        stream.write_all(body)?;
    }
    stream.flush()?;
    Ok(())
}

fn load_native_http_config(args: &[String]) -> NativeHttpConfig {
    let mut config = NativeHttpConfig::default();

    let config_dir = match extract_flag(args, "-config_dir") {
        Some(v) if !v.is_empty() => v,
        _ => return config,
    };
    let security_path = PathBuf::from(config_dir).join("security.toml");
    let content = match std::fs::read_to_string(security_path) {
        Ok(v) => v,
        Err(_) => return config,
    };

    let mut section = "";
    for raw_line in content.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            section = line;
            if section == "[jwt.signing]" {
                config.jwt_signing_enabled = true;
            }
            continue;
        }
        if section == "[access]" {
            if let Some((name, value)) = line.split_once('=') {
                if name.trim().eq_ignore_ascii_case("ui")
                    && value.trim().eq_ignore_ascii_case("true")
                {
                    config.access_ui_enabled = true;
                }
            }
        }
    }
    config
}

fn is_ui_access_denied(config: Option<&NativeHttpConfig>) -> bool {
    if let Some(c) = config {
        return c.jwt_signing_enabled && !c.access_ui_enabled;
    }
    false
}

fn native_ui_index_html() -> &'static str {
    "<html><head><title>SeaweedFS Volume</title></head><body><h1>SeaweedFS Volume</h1></body></html>"
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
    println!("  - native (default): bootstrap native mode entrypoint (currently supervises/proxies Go backend)");
    println!("  - exec: exec Go weed volume process directly");
    println!("  - proxy: run Rust TCP proxy front-end and supervise Go weed backend");
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
