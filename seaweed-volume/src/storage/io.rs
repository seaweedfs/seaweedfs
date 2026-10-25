//! Positional file reads.
//!
//! Every read here is "these bytes at this offset", never "the next bytes".
//! The handles are shared — `.dat` and `.idx` descriptors are borrowed from
//! [`file_pool`](super::needle_map::file_pool), a mounted EC shard's handle is
//! duplicated into a scrub plan — so no caller may rely on a file position.
//!
//! On unix that is `pread(2)` through `std::os::unix::fs::FileExt`. On Windows
//! it is `seek_read`, which passes the offset through `OVERLAPPED`, so the read
//! itself is independent of the current cursor.
//!
//! What these helpers replace is `try_clone()` + `seek()` + `read()`. A
//! duplicated handle shares one kernel file offset with the original, so that
//! sequence is two syscalls against state another thread can move in between:
//! the seek positions the offset, a concurrent reader or an append moves it,
//! and the read returns bytes from somewhere else entirely. `seek_read` carries
//! its own offset in a single call, so there is no window.
//!
//! `seek_read` does still advance the cursor as a side effect — Windows updates
//! the file pointer even for an `OVERLAPPED` read — which nothing here relies
//! on. A caller that genuinely needs a private position must open the file
//! again rather than duplicate a handle; see `Volume::dat_scan_plan` in
//! [`storage::volume`](super::volume).

use std::fs::File;
use std::io;

/// Reads exactly `buf.len()` bytes from `file` starting at `offset`.
///
/// Fails with [`io::ErrorKind::UnexpectedEof`] if the file ends first.
pub(crate) fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(buf, offset)?;
    }
    #[cfg(windows)]
    {
        if read_full_at(file, buf, offset)? < buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF in seek_read",
            ));
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        compile_error!("Platform not supported: only unix and windows are supported");
    }
    Ok(())
}

/// Reads up to `buf.len()` bytes from `file` starting at `offset`, returning
/// how many were read.
///
/// A short read — including `0` at or past end of file — is not an error; use
/// [`read_exact_at`] when the whole buffer must be filled.
pub(crate) fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_at(buf, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        file.seek_read(buf, offset)
    }
    #[cfg(not(any(unix, windows)))]
    {
        compile_error!("Platform not supported: only unix and windows are supported");
    }
}

/// Reads into `buf` at `offset` until it is full or the file ends, retrying
/// interrupted reads; returns how many bytes were read.
///
/// Unlike [`read_at`], a count below `buf.len()` always means end of file.
pub(crate) fn read_full_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    fill_at(|b, at| read_at(file, b, at), buf, offset)
}

fn fill_at(
    mut read: impl FnMut(&mut [u8], u64) -> io::Result<usize>,
    buf: &mut [u8],
    offset: u64,
) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match read(&mut buf[filled..], offset + filled as u64) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
    Ok(filled)
}

#[cfg(test)]
mod tests {
    use super::{fill_at, read_at, read_exact_at, read_full_at};
    use std::io::{ErrorKind, Write};

    fn temp_file(bytes: &[u8]) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().expect("temp file");
        f.write_all(bytes).expect("write");
        f.flush().expect("flush");
        f
    }

    #[test]
    fn read_exact_at_fills_the_whole_buffer() {
        let f = temp_file(b"0123456789");
        let mut buf = [0u8; 10];
        read_exact_at(f.as_file(), &mut buf, 0).expect("read");
        assert_eq!(&buf, b"0123456789");
    }

    #[test]
    fn read_exact_at_reads_from_the_offset() {
        let f = temp_file(b"0123456789");
        let mut buf = [0u8; 4];
        read_exact_at(f.as_file(), &mut buf, 3).expect("read");
        assert_eq!(&buf, b"3456");

        // The helper is positional: a second read at a lower offset sees the
        // bytes at that offset, not wherever the first read left a cursor.
        let mut again = [0u8; 4];
        read_exact_at(f.as_file(), &mut again, 1).expect("read");
        assert_eq!(&again, b"1234");
    }

    #[test]
    fn read_exact_at_short_file_is_unexpected_eof() {
        let f = temp_file(b"0123");
        let mut buf = [0u8; 8];
        let err = read_exact_at(f.as_file(), &mut buf, 0).expect_err("short file");
        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
    }

    #[test]
    fn read_at_allows_a_short_read_at_eof() {
        let f = temp_file(b"0123456789");
        let mut buf = [0u8; 8];

        let n = read_at(f.as_file(), &mut buf, 6).expect("read");
        assert_eq!(n, 4);
        assert_eq!(&buf[..n], b"6789");

        // Entirely past the end is zero bytes, not an error.
        let n = read_at(f.as_file(), &mut buf, 10).expect("read");
        assert_eq!(n, 0);
    }

    /// A source that returns at most `chunk` bytes per call and fails with
    /// `Interrupted` on its first call, like a network mount under a signal.
    fn chunked(src: &[u8], chunk: usize) -> impl FnMut(&mut [u8], u64) -> std::io::Result<usize> {
        let mut interrupted = false;
        move |buf, at| {
            if !interrupted {
                interrupted = true;
                return Err(ErrorKind::Interrupted.into());
            }
            let at = (at as usize).min(src.len());
            let n = buf.len().min(chunk).min(src.len() - at);
            buf[..n].copy_from_slice(&src[at..at + n]);
            Ok(n)
        }
    }

    #[test]
    fn fill_at_fills_across_short_and_interrupted_reads() {
        let src: Vec<u8> = (0..=255).collect();
        let mut buf = [0u8; 100];
        let n = fill_at(chunked(&src, 7), &mut buf, 50).expect("read");
        assert_eq!(n, buf.len());
        assert_eq!(&buf[..], &src[50..150]);
    }

    #[test]
    fn fill_at_stops_at_end_of_source() {
        let src: Vec<u8> = (0..=255).collect();
        let mut buf = [0u8; 100];
        let n = fill_at(chunked(&src, 7), &mut buf, 200).expect("read");
        assert_eq!(n, 56);
        assert_eq!(&buf[..n], &src[200..]);
    }

    #[test]
    fn fill_at_propagates_other_errors() {
        let mut buf = [0u8; 8];
        let err = fill_at(|_, _| Err(ErrorKind::PermissionDenied.into()), &mut buf, 0)
            .expect_err("error");
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
    }

    #[test]
    fn read_full_at_returns_the_short_count_only_at_eof() {
        let f = temp_file(b"0123456789");
        let mut buf = [0u8; 8];
        assert_eq!(read_full_at(f.as_file(), &mut buf, 0).expect("read"), 8);
        assert_eq!(&buf, b"01234567");
        assert_eq!(read_full_at(f.as_file(), &mut buf, 6).expect("read"), 4);
        assert_eq!(&buf[..4], b"6789");
        assert_eq!(read_full_at(f.as_file(), &mut buf, 10).expect("read"), 0);
    }
}
