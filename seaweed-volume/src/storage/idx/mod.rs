//! Index file (.idx) format: sequential 17-byte entries.
//!
//! Each entry: NeedleId(8) + Offset(5) + Size(4) = 17 bytes.

use crate::storage::types::*;
use std::io::{self, Read, Seek, SeekFrom};

const ROWS_TO_READ: usize = 1024;

/// Walk all entries in an .idx file, calling `f` for each.
/// Mirrors Go's `WalkIndexFile()`.
pub fn walk_index_file<R, F>(reader: &mut R, start_from: u64, mut f: F) -> io::Result<()>
where
    R: Read + Seek,
    F: FnMut(NeedleId, Offset, Size) -> io::Result<()>,
{
    let reader_offset = start_from * NEEDLE_MAP_ENTRY_SIZE as u64;
    reader.seek(SeekFrom::Start(reader_offset))?;

    let mut buf = vec![0u8; NEEDLE_MAP_ENTRY_SIZE * ROWS_TO_READ];

    loop {
        let count = match reader.read(&mut buf) {
            Ok(0) => return Ok(()),
            Ok(n) => n,
            Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        };

        let mut i = 0;
        while i + NEEDLE_MAP_ENTRY_SIZE <= count {
            let (key, offset, size) = idx_entry_from_bytes(&buf[i..i + NEEDLE_MAP_ENTRY_SIZE]);
            f(key, offset, size)?;
            i += NEEDLE_MAP_ENTRY_SIZE;
        }
    }
}

/// Write a single index entry to a writer.
pub fn write_index_entry<W: io::Write>(writer: &mut W, key: NeedleId, offset: Offset, size: Size) -> io::Result<()> {
    let mut buf = [0u8; NEEDLE_MAP_ENTRY_SIZE];
    idx_entry_to_bytes(&mut buf, key, offset, size);
    writer.write_all(&buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_walk_index_file() {
        // Create a small index with 3 entries
        let mut data = Vec::new();
        let entries = vec![
            (NeedleId(1), Offset::from_actual_offset(0), Size(100)),
            (NeedleId(2), Offset::from_actual_offset(128), Size(200)),
            (NeedleId(3), Offset::from_actual_offset(384), Size(300)),
        ];
        for (key, offset, size) in &entries {
            let mut buf = [0u8; NEEDLE_MAP_ENTRY_SIZE];
            idx_entry_to_bytes(&mut buf, *key, *offset, *size);
            data.extend_from_slice(&buf);
        }

        let mut cursor = Cursor::new(data);
        let mut collected = Vec::new();
        walk_index_file(&mut cursor, 0, |key, offset, size| {
            collected.push((key, offset.to_actual_offset(), size));
            Ok(())
        }).unwrap();

        assert_eq!(collected.len(), 3);
        assert_eq!(collected[0].0, NeedleId(1));
        assert_eq!(collected[0].1, 0);
        assert_eq!(collected[0].2, Size(100));
        assert_eq!(collected[1].0, NeedleId(2));
        assert_eq!(collected[2].0, NeedleId(3));
    }

    #[test]
    fn test_walk_empty() {
        let mut cursor = Cursor::new(Vec::new());
        let mut count = 0;
        walk_index_file(&mut cursor, 0, |_, _, _| { count += 1; Ok(()) }).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_write_index_entry() {
        let mut buf = Vec::new();
        write_index_entry(&mut buf, NeedleId(42), Offset::from_actual_offset(8 * 10), Size(512)).unwrap();
        assert_eq!(buf.len(), NEEDLE_MAP_ENTRY_SIZE);

        let (key, offset, size) = idx_entry_from_bytes(&buf);
        assert_eq!(key, NeedleId(42));
        assert_eq!(offset.to_actual_offset(), 80);
        assert_eq!(size, Size(512));
    }
}
