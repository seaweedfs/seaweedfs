use std::fmt::Write as _;

use crate::server::server_stats;
use crate::server::volume_server::VolumeServerState;
use crate::storage::store::Store;

pub struct EmbeddedAsset {
    pub content_type: &'static str,
    pub bytes: &'static [u8],
}

struct UiDiskRow {
    dir: String,
    disk_type: String,
    all: u64,
    free: u64,
    used: u64,
}

struct UiVolumeRow {
    id: u32,
    collection: String,
    disk_type: String,
    size: u64,
    file_count: i64,
    delete_count: i64,
    deleted_byte_count: u64,
    ttl: String,
    read_only: bool,
    version: u32,
    remote_storage_name: String,
    remote_storage_key: String,
}

struct UiEcShardRow {
    shard_id: u8,
    size: u64,
}

struct UiEcVolumeRow {
    volume_id: u32,
    collection: String,
    size: u64,
    shards: Vec<UiEcShardRow>,
    created_at: String,
}

pub fn favicon_asset() -> EmbeddedAsset {
    EmbeddedAsset {
        content_type: "image/x-icon",
        bytes: include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../weed/static/favicon.ico"
        )),
    }
}

pub fn lookup_static_asset(path: &str) -> Option<EmbeddedAsset> {
    let path = path.trim_start_matches('/');
    let asset = match path {
        "bootstrap/3.3.1/css/bootstrap.min.css" => EmbeddedAsset {
            content_type: "text/css; charset=utf-8",
            bytes: include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../weed/static/bootstrap/3.3.1/css/bootstrap.min.css"
            )),
        },
        "bootstrap/3.3.1/fonts/glyphicons-halflings-regular.eot" => EmbeddedAsset {
            content_type: "application/vnd.ms-fontobject",
            bytes: include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../weed/static/bootstrap/3.3.1/fonts/glyphicons-halflings-regular.eot"
            )),
        },
        "bootstrap/3.3.1/fonts/glyphicons-halflings-regular.svg" => EmbeddedAsset {
            content_type: "image/svg+xml",
            bytes: include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../weed/static/bootstrap/3.3.1/fonts/glyphicons-halflings-regular.svg"
            )),
        },
        "bootstrap/3.3.1/fonts/glyphicons-halflings-regular.ttf" => EmbeddedAsset {
            content_type: "font/ttf",
            bytes: include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../weed/static/bootstrap/3.3.1/fonts/glyphicons-halflings-regular.ttf"
            )),
        },
        "bootstrap/3.3.1/fonts/glyphicons-halflings-regular.woff" => EmbeddedAsset {
            content_type: "font/woff",
            bytes: include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../weed/static/bootstrap/3.3.1/fonts/glyphicons-halflings-regular.woff"
            )),
        },
        "images/folder.gif" => EmbeddedAsset {
            content_type: "image/gif",
            bytes: include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../weed/static/images/folder.gif"
            )),
        },
        "javascript/jquery-3.6.0.min.js" => EmbeddedAsset {
            content_type: "application/javascript; charset=utf-8",
            bytes: include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../weed/static/javascript/jquery-3.6.0.min.js"
            )),
        },
        "javascript/jquery-sparklines/2.1.2/jquery.sparkline.min.js" => EmbeddedAsset {
            content_type: "application/javascript; charset=utf-8",
            bytes: include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../weed/static/javascript/jquery-sparklines/2.1.2/jquery.sparkline.min.js"
            )),
        },
        "seaweed50x50.png" => EmbeddedAsset {
            content_type: "image/png",
            bytes: include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../weed/static/seaweed50x50.png"
            )),
        },
        _ => return None,
    };
    Some(asset)
}

pub fn render_volume_server_html(state: &VolumeServerState) -> String {
    let counters = server_stats::snapshot();
    let (disk_rows, volume_rows, remote_volume_rows, ec_volume_rows) = {
        let store = state.store.read().unwrap();
        collect_ui_data(&store)
    };

    let masters = if state.master_urls.is_empty() {
        "[]".to_string()
    } else {
        format!("[{}]", state.master_urls.join(" "))
    };
    let uptime = server_stats::uptime_string();
    let read_week = join_i64(&counters.read_requests.week_counter.to_list());
    let read_day = join_i64(&counters.read_requests.day_counter.to_list());
    let read_hour = join_i64(&counters.read_requests.hour_counter.to_list());
    let read_minute = join_i64(&counters.read_requests.minute_counter.to_list());

    let mut disk_rows_html = String::new();
    for disk in &disk_rows {
        let _ = write!(
            disk_rows_html,
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{:.2}%</td></tr>",
            escape_html(&disk.dir),
            escape_html(&disk.disk_type),
            bytes_to_human_readable(disk.all),
            bytes_to_human_readable(disk.free),
            percent_from(disk.all, disk.used),
        );
    }

    let mut volume_rows_html = String::new();
    for volume in &volume_rows {
        let _ = write!(
            volume_rows_html,
            "<tr><td><code>{}</code></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{} / {}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            volume.id,
            escape_html(&volume.collection),
            escape_html(&volume.disk_type),
            bytes_to_human_readable(volume.size),
            volume.file_count,
            volume.delete_count,
            bytes_to_human_readable(volume.deleted_byte_count),
            escape_html(&volume.ttl),
            volume.read_only,
            volume.version,
        );
    }

    let remote_section = if remote_volume_rows.is_empty() {
        String::new()
    } else {
        let mut remote_rows_html = String::new();
        for volume in &remote_volume_rows {
            let _ = write!(
                remote_rows_html,
                "<tr><td><code>{}</code></td><td>{}</td><td>{}</td><td>{}</td><td>{} / {}</td><td>{}</td><td>{}</td></tr>",
                volume.id,
                escape_html(&volume.collection),
                bytes_to_human_readable(volume.size),
                volume.file_count,
                volume.delete_count,
                bytes_to_human_readable(volume.deleted_byte_count),
                escape_html(&volume.remote_storage_name),
                escape_html(&volume.remote_storage_key),
            );
        }
        format!(
            r#"<div class="row">
            <h2>Remote Volumes</h2>
            <table class="table table-striped">
                <thead>
                    <tr>
                        <th>Id</th>
                        <th>Collection</th>
                        <th>Size</th>
                        <th>Files</th>
                        <th>Trash</th>
                        <th>Remote</th>
                        <th>Key</th>
                    </tr>
                </thead>
                <tbody>{}</tbody>
            </table>
        </div>"#,
            remote_rows_html
        )
    };

    let ec_section = if ec_volume_rows.is_empty() {
        String::new()
    } else {
        let mut ec_rows_html = String::new();
        for ec in &ec_volume_rows {
            let mut shard_labels = String::new();
            for shard in &ec.shards {
                let _ = write!(
                    shard_labels,
                    "<span class=\"label label-info\" style=\"margin-right: 5px;\">{}: {}</span>",
                    shard.shard_id,
                    bytes_to_human_readable(shard.size)
                );
            }
            let _ = write!(
                ec_rows_html,
                "<tr><td><code>{}</code></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                ec.volume_id,
                escape_html(&ec.collection),
                bytes_to_human_readable(ec.size),
                shard_labels,
                escape_html(&ec.created_at),
            );
        }
        format!(
            r#"<div class="row">
            <h2>Erasure Coding Shards</h2>
            <table class="table table-striped">
                <thead>
                    <tr>
                        <th>Id</th>
                        <th>Collection</th>
                        <th>Total Size</th>
                        <th>Shard Details</th>
                        <th>CreatedAt</th>
                    </tr>
                </thead>
                <tbody>{}</tbody>
            </table>
        </div>"#,
            ec_rows_html
        )
    };

    format!(
        r#"<!DOCTYPE html>
<html>
<head>
    <title>SeaweedFS {version}</title>
    <link rel="stylesheet" href="/seaweedfsstatic/bootstrap/3.3.1/css/bootstrap.min.css">
    <script type="text/javascript" src="/seaweedfsstatic/javascript/jquery-3.6.0.min.js"></script>
    <script type="text/javascript" src="/seaweedfsstatic/javascript/jquery-sparklines/2.1.2/jquery.sparkline.min.js"></script>
    <script type="text/javascript">
        $(function () {{
            var periods = ['second', 'minute', 'hour', 'day'];
            for (var i = 0; i < periods.length; i++) {{
                var period = periods[i];
                $('.inlinesparkline-' + period).sparkline('html', {{
                    type: 'line',
                    barColor: 'red',
                    tooltipSuffix: ' request per ' + period
                }});
            }}
        }});
    </script>
    <style>
        #jqstooltip {{
            height: 28px !important;
            width: 150px !important;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="page-header">
            <h1>
                <a href="https://github.com/seaweedfs/seaweedfs"><img src="/seaweedfsstatic/seaweed50x50.png" alt="SeaweedFS"></a>
                SeaweedFS <small>{version}</small>
            </h1>
        </div>

        <div class="row">
            <div class="col-sm-6">
                <h2>Disk Stats</h2>
                <table class="table table-striped">
                    <thead>
                        <tr>
                            <th>Path</th>
                            <th>Disk</th>
                            <th>Total</th>
                            <th>Free</th>
                            <th>Usage</th>
                        </tr>
                    </thead>
                    <tbody>{disk_rows_html}</tbody>
                </table>
            </div>

            <div class="col-sm-6">
                <h2>System Stats</h2>
                <table class="table table-condensed table-striped">
                    <tr><th>Masters</th><td>{masters}</td></tr>
                    <tr><th>Weekly # ReadRequests</th><td><span class="inlinesparkline-day">{read_week}</span></td></tr>
                    <tr><th>Daily # ReadRequests</th><td><span class="inlinesparkline-hour">{read_day}</span></td></tr>
                    <tr><th>Hourly # ReadRequests</th><td><span class="inlinesparkline-minute">{read_hour}</span></td></tr>
                    <tr><th>Last Minute # ReadRequests</th><td><span class="inlinesparkline-second">{read_minute}</span></td></tr>
                    <tr><th>Up Time</th><td>{uptime}</td></tr>
                </table>
            </div>
        </div>

        <div class="row">
            <h2>Volumes</h2>
            <table class="table table-striped">
                <thead>
                    <tr>
                        <th>Id</th>
                        <th>Collection</th>
                        <th>Disk</th>
                        <th>Data Size</th>
                        <th>Files</th>
                        <th>Trash</th>
                        <th>TTL</th>
                        <th>ReadOnly</th>
                        <th>Version</th>
                    </tr>
                </thead>
                <tbody>{volume_rows_html}</tbody>
            </table>
        </div>

        {remote_section}
        {ec_section}
    </div>
</body>
</html>"#,
        version = escape_html(crate::version::version()),
        disk_rows_html = disk_rows_html,
        masters = escape_html(&masters),
        read_week = read_week,
        read_day = read_day,
        read_hour = read_hour,
        read_minute = read_minute,
        uptime = escape_html(&uptime),
        volume_rows_html = volume_rows_html,
        remote_section = remote_section,
        ec_section = ec_section,
    )
}

fn collect_ui_data(store: &Store) -> (Vec<UiDiskRow>, Vec<UiVolumeRow>, Vec<UiVolumeRow>, Vec<UiEcVolumeRow>) {
    let mut disk_rows = Vec::new();
    let mut volumes = Vec::new();
    let mut remote_volumes = Vec::new();
    let mut ec_volumes = Vec::new();

    for loc in &store.locations {
        let dir = absolute_display_path(&loc.directory);
        let (all, free) = crate::storage::disk_location::get_disk_stats(&dir);
        disk_rows.push(UiDiskRow {
            dir,
            disk_type: loc.disk_type.to_string(),
            all,
            free,
            used: all.saturating_sub(free),
        });

        for (_, volume) in loc.volumes() {
            let (remote_storage_name, remote_storage_key) = volume.remote_storage_name_key();
            let row = UiVolumeRow {
                id: volume.id.0,
                collection: volume.collection.clone(),
                disk_type: loc.disk_type.to_string(),
                size: volume.content_size(),
                file_count: volume.file_count(),
                delete_count: volume.deleted_count(),
                deleted_byte_count: volume.deleted_size(),
                ttl: volume.super_block.ttl.to_string(),
                read_only: volume.is_read_only(),
                version: volume.version().0 as u32,
                remote_storage_name,
                remote_storage_key,
            };
            if row.remote_storage_name.is_empty() {
                volumes.push(row);
            } else {
                remote_volumes.push(row);
            }
        }

        for (_, ec_volume) in loc.ec_volumes() {
            let mut shards = Vec::new();
            let mut total_size = 0u64;
            let mut created_at = String::from("-");
            for shard in ec_volume.shards.iter().flatten() {
                let shard_size = shard.file_size().max(0) as u64;
                total_size = total_size.saturating_add(shard_size);
                shards.push(UiEcShardRow {
                    shard_id: shard.shard_id,
                    size: shard_size,
                });
                if created_at == "-" {
                    if let Ok(metadata) = std::fs::metadata(shard.file_name()) {
                        if let Ok(modified) = metadata.modified() {
                            let ts: chrono::DateTime<chrono::Local> = modified.into();
                            created_at = ts.format("%Y-%m-%d %H:%M").to_string();
                        }
                    }
                }
            }
            let preferred_size = ec_volume.dat_file_size.max(0) as u64;
            ec_volumes.push(UiEcVolumeRow {
                volume_id: ec_volume.volume_id.0,
                collection: ec_volume.collection.clone(),
                size: preferred_size.max(total_size),
                shards,
                created_at,
            });
        }
    }

    disk_rows.sort_by(|left, right| left.dir.cmp(&right.dir));
    volumes.sort_by_key(|row| row.id);
    remote_volumes.sort_by_key(|row| row.id);
    ec_volumes.sort_by_key(|row| row.volume_id);

    (disk_rows, volumes, remote_volumes, ec_volumes)
}

fn absolute_display_path(path: &str) -> String {
    let p = std::path::Path::new(path);
    if p.is_absolute() {
        return path.to_string();
    }
    std::env::current_dir()
        .map(|cwd| cwd.join(p).to_string_lossy().to_string())
        .unwrap_or_else(|_| path.to_string())
}

fn join_i64(values: &[i64]) -> String {
    values
        .iter()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn percent_from(total: u64, part: u64) -> f64 {
    if total == 0 {
        return 0.0;
    }
    (part as f64 / total as f64) * 100.0
}

fn bytes_to_human_readable(bytes: u64) -> String {
    const UNIT: u64 = 1024;
    if bytes < UNIT {
        return format!("{} B", bytes);
    }

    let mut div = UNIT;
    let mut exp = 0usize;
    let mut n = bytes / UNIT;
    while n >= UNIT {
        div *= UNIT;
        n /= UNIT;
        exp += 1;
    }

    format!("{:.2} {}iB", bytes as f64 / div as f64, ["K", "M", "G", "T", "P", "E"][exp])
}

fn escape_html(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

