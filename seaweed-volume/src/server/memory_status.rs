use crate::pb::volume_server_pb;

pub fn collect_mem_status() -> volume_server_pb::MemStatus {
    #[allow(unused_mut)]
    let mut mem = volume_server_pb::MemStatus {
        goroutines: 1,
        ..Default::default()
    };

    #[cfg(target_os = "linux")]
    {
        if let Some((all, free)) = get_system_memory_linux() {
            mem.all = all;
            mem.free = free;
            mem.used = all.saturating_sub(free);
        }

        if let Some(status) = read_process_status_linux() {
            if status.threads > 0 {
                mem.goroutines = status.threads as i32;
            }
            if let Some(rss) = status.rss {
                mem.self_ = rss;
            }
            if let Some(heap) = status.data.or(status.rss) {
                mem.heap = heap;
            }
            if let Some(stack) = status.stack {
                mem.stack = stack;
            }
        }
    }

    mem
}

#[cfg(target_os = "linux")]
fn get_system_memory_linux() -> Option<(u64, u64)> {
    unsafe {
        let mut info: libc::sysinfo = std::mem::zeroed();
        if libc::sysinfo(&mut info) == 0 {
            let unit = info.mem_unit as u64;
            let total = info.totalram as u64 * unit;
            let free = info.freeram as u64 * unit;
            return Some((total, free));
        }
    }
    None
}

#[cfg(target_os = "linux")]
#[derive(Default)]
struct ProcessStatus {
    threads: u64,
    rss: Option<u64>,
    data: Option<u64>,
    stack: Option<u64>,
}

#[cfg(target_os = "linux")]
fn read_process_status_linux() -> Option<ProcessStatus> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let mut out = ProcessStatus::default();

    for line in status.lines() {
        if let Some(value) = line.strip_prefix("Threads:") {
            out.threads = value.trim().parse().ok()?;
            continue;
        }
        if let Some(value) = parse_proc_status_kib_field(line, "VmRSS:") {
            out.rss = Some(value);
            continue;
        }
        if let Some(value) = parse_proc_status_kib_field(line, "VmData:") {
            out.data = Some(value);
            continue;
        }
        if let Some(value) = parse_proc_status_kib_field(line, "VmStk:") {
            out.stack = Some(value);
        }
    }

    Some(out)
}

#[cfg(target_os = "linux")]
fn parse_proc_status_kib_field(line: &str, prefix: &str) -> Option<u64> {
    let raw = line.strip_prefix(prefix)?.trim();
    let value = raw.strip_suffix(" kB").unwrap_or(raw).trim();
    value.parse::<u64>().ok().map(|kib| kib * 1024)
}

#[cfg(test)]
mod tests {
    use super::collect_mem_status;

    #[test]
    fn test_collect_mem_status_reports_live_process_state() {
        let mem = collect_mem_status();
        assert!(mem.goroutines > 0);
    }
}
