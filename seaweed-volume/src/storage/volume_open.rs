//! Opening volume data and index files without access-time updates.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

/// Open a volume data or index file with `O_NOATIME`. Nothing reads these
/// files' atime, but without the flag every needle read dirties the inode —
/// even relatime writes atime on the first read after each write, so an
/// actively written volume pays a metadata write per read/write cycle.
/// Matches Go's `backend.OpenVolumeFile`.
pub fn open_volume_file(opts: &OpenOptions, path: impl AsRef<Path>) -> io::Result<File> {
    let path = path.as_ref();
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut noatime = opts.clone();
        noatime.custom_flags(libc::O_NOATIME);
        match noatime.open(path) {
            // O_NOATIME is refused unless we own the file or hold CAP_FOWNER.
            Err(e) if e.raw_os_error() == Some(libc::EPERM) => opts.open(path),
            result => result,
        }
    }
    #[cfg(not(target_os = "linux"))]
    opts.open(path)
}
