//! Index file (.idx) format: sequential 17-byte entries.
//!
//! Each entry: NeedleId(8) + Offset(5) + Size(4) = 17 bytes.

use crate::storage::needle::needle::get_actual_size;
use crate::storage::types::*;
use std::io::{self, Read, Seek, SeekFrom};

pub(crate) const ROWS_TO_READ: usize = 1024;

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
        // Fill the whole batch before decoding. `Read::read` may return a short
        // count that is not a multiple of the entry size (FUSE/network files, a
        // BufReader with an odd capacity); decoding it as-is would drop the split
        // entry and misalign every later row. Go is immune: `ReadAt` returns a
        // full buffer or an error.
        let mut count = 0;
        let mut eof = false;
        while count < buf.len() {
            match reader.read(&mut buf[count..]) {
                Ok(0) => {
                    eof = true;
                    break;
                }
                Ok(n) => count += n,
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    eof = true;
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        let mut i = 0;
        while i + NEEDLE_MAP_ENTRY_SIZE <= count {
            let (key, offset, size) = idx_entry_from_bytes(&buf[i..i + NEEDLE_MAP_ENTRY_SIZE]);
            f(key, offset, size)?;
            i += NEEDLE_MAP_ENTRY_SIZE;
        }

        // A trailing partial entry at EOF is ignored, as Go does on `io.EOF`.
        if eof {
            return Ok(());
        }
    }
}

/// Verify the integrity of an .idx/.ecx index file: walk every entry, sort by
/// (offset, size), flag overlapping needles, and check the file is a whole
/// number of entries. Mirrors Go's `idx.CheckIndexFile`. No data-file reads.
pub fn check_index_file<R: Read + Seek>(
    reader: &mut R,
    idx_file_size: i64,
    version: Version,
) -> (u64, Vec<String>) {
    let mut errs = Vec::new();
    // (walk index, id, actual offset, size)
    let mut entries: Vec<(usize, NeedleId, i64, Size)> = Vec::new();
    let mut i = 0usize;
    if let Err(e) = walk_index_file(reader, 0, |id, offset, size| {
        entries.push((i, id, offset.to_actual_offset(), size));
        i += 1;
        Ok(())
    }) {
        errs.push(format!("walk index file: {}", e));
    }

    entries.sort_by(|a, b| a.2.cmp(&b.2).then(a.3.0.cmp(&b.3.0)));

    // Offset-0 logical tombstones (remote-tier deletes) occupy no physical extent,
    // so they cannot overlap anything — exclude them from the overlap check. They
    // are still counted below for the index-size check.
    let physical: Vec<(usize, NeedleId, i64, Size)> = entries
        .iter()
        .copied()
        .filter(|e| !(e.2 == 0 && e.3.is_deleted()))
        .collect();

    for j in 1..physical.len() {
        let (index, id, offset, size) = physical[j];
        let (_, last_id, last_offset, last_size) = physical[j - 1];

        let end = match get_actual_size(size, version) {
            0 => offset,
            s => offset + s - 1,
        };
        let last_end = match get_actual_size(last_size, version) {
            0 => last_offset,
            s => last_offset + s - 1,
        };

        if offset <= last_end {
            errs.push(format!(
                "needle {} (#{}) at [{}-{}] overlaps needle {} at [{}-{}]",
                id.0,
                index + 1,
                offset,
                end,
                last_id.0,
                last_offset,
                last_end
            ));
        }
    }

    let count = entries.len() as u64;
    let expected = count as i64 * NEEDLE_MAP_ENTRY_SIZE as i64;
    if expected != idx_file_size {
        errs.push(format!(
            "expected an index file of size {}, got {}",
            idx_file_size, expected
        ));
    }

    (count, errs)
}

/// Write a single index entry to a writer.
pub fn write_index_entry<W: io::Write>(
    writer: &mut W,
    key: NeedleId,
    offset: Offset,
    size: Size,
) -> io::Result<()> {
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
        })
        .unwrap();

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
        walk_index_file(&mut cursor, 0, |_, _, _| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 0);
    }

    fn idx_bytes(entries: &[(NeedleId, Offset, Size)]) -> Vec<u8> {
        let mut data = Vec::new();
        for (key, offset, size) in entries {
            let mut buf = [0u8; NEEDLE_MAP_ENTRY_SIZE];
            idx_entry_to_bytes(&mut buf, *key, *offset, *size);
            data.extend_from_slice(&buf);
        }
        data
    }

    /// Reader that hands back at most `chunk` bytes per `read`, as a FUSE or
    /// network file may. 7 is coprime with the 17-byte entry size, so nearly
    /// every read ends mid-entry. With `interrupts`, every other call fails
    /// with `ErrorKind::Interrupted` instead.
    struct ShortReader {
        inner: Cursor<Vec<u8>>,
        chunk: usize,
        interrupts: bool,
        interrupt_next: bool,
    }

    impl ShortReader {
        fn new(data: Vec<u8>, interrupts: bool) -> Self {
            ShortReader {
                inner: Cursor::new(data),
                chunk: 7,
                interrupts,
                interrupt_next: false,
            }
        }
    }

    impl Read for ShortReader {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if self.interrupt_next {
                self.interrupt_next = false;
                return Err(io::Error::from(io::ErrorKind::Interrupted));
            }
            self.interrupt_next = self.interrupts;
            let n = buf.len().min(self.chunk);
            self.inner.read(&mut buf[..n])
        }
    }

    impl Seek for ShortReader {
        fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
            self.inner.seek(pos)
        }
    }

    fn walk_all<R: Read + Seek>(reader: &mut R, start_from: u64) -> Vec<(NeedleId, i64, Size)> {
        let mut collected = Vec::new();
        walk_index_file(reader, start_from, |key, offset, size| {
            collected.push((key, offset.to_actual_offset(), size));
            Ok(())
        })
        .unwrap();
        collected
    }

    /// More than one ROWS_TO_READ batch, so the walk crosses a buffer refill.
    fn many_entries() -> Vec<(NeedleId, Offset, Size)> {
        (0..(ROWS_TO_READ as u64 * 2 + 37))
            .map(|i| {
                (
                    NeedleId(i * 7 + 1),
                    Offset::from_actual_offset(i as i64 * 128),
                    Size(i as i32 + 1),
                )
            })
            .collect()
    }

    #[test]
    fn test_walk_index_file_short_reads_keep_alignment() {
        let data = idx_bytes(&many_entries());
        let expected = walk_all(&mut Cursor::new(data.clone()), 0);
        assert_eq!(expected.len(), ROWS_TO_READ * 2 + 37);

        let mut short = ShortReader::new(data, false);
        assert_eq!(walk_all(&mut short, 0), expected);
    }

    #[test]
    fn test_walk_index_file_retries_interrupted_reads() {
        let data = idx_bytes(&many_entries());
        let expected = walk_all(&mut Cursor::new(data.clone()), 0);

        let mut short = ShortReader::new(data, true);
        assert_eq!(walk_all(&mut short, 0), expected);
    }

    #[test]
    fn test_walk_index_file_short_reads_start_from() {
        let data = idx_bytes(&many_entries());
        let expected = walk_all(&mut Cursor::new(data.clone()), 0);

        let start = ROWS_TO_READ as u64 + 5;
        let mut short = ShortReader::new(data, false);
        assert_eq!(walk_all(&mut short, start), expected[start as usize..]);
    }

    #[test]
    fn test_walk_index_file_ignores_trailing_partial_entry() {
        // A torn final entry is dropped without an error, as Go does on io.EOF.
        let entries = many_entries();
        let mut data = idx_bytes(&entries);
        data.extend_from_slice(&[0xAB; NEEDLE_MAP_ENTRY_SIZE - 1]);

        let expected = walk_all(&mut Cursor::new(idx_bytes(&entries)), 0);
        assert_eq!(walk_all(&mut Cursor::new(data.clone()), 0), expected);

        let mut short = ShortReader::new(data, false);
        assert_eq!(walk_all(&mut short, 0), expected);
    }

    #[test]
    fn test_check_index_file_clean() {
        let data = idx_bytes(&[
            (NeedleId(1), Offset::from_actual_offset(0), Size(50)),
            (NeedleId(2), Offset::from_actual_offset(100_000), Size(50)),
        ]);
        let size = data.len() as i64;
        let (count, errs) = check_index_file(&mut Cursor::new(data), size, Version(3));
        assert_eq!(count, 2);
        assert!(errs.is_empty(), "{:?}", errs);
    }

    #[test]
    fn test_check_index_file_detects_overlap() {
        let data = idx_bytes(&[
            (NeedleId(1), Offset::from_actual_offset(0), Size(100)),
            (NeedleId(2), Offset::from_actual_offset(8), Size(100)),
        ]);
        let size = data.len() as i64;
        let (count, errs) = check_index_file(&mut Cursor::new(data), size, Version(3));
        assert_eq!(count, 2);
        assert_eq!(errs.len(), 1, "{:?}", errs);
        assert!(errs[0].contains("overlaps"), "{:?}", errs);
    }

    #[test]
    fn test_check_index_file_ignores_offset0_tombstone() {
        // A live needle near the start plus an offset-0 logical tombstone (remote-tier
        // delete) must NOT be flagged as overlapping; the tombstone row still counts.
        let data = idx_bytes(&[
            (NeedleId(1), Offset::from_actual_offset(8), Size(100)),
            (NeedleId(2), Offset::from_actual_offset(0), Size(-1)),
        ]);
        let size = data.len() as i64;
        let (count, errs) = check_index_file(&mut Cursor::new(data), size, Version(3));
        assert_eq!(count, 2, "tombstone row is still counted: {:?}", errs);
        assert!(
            errs.is_empty(),
            "offset-0 tombstone must not overlap: {:?}",
            errs
        );
    }

    #[test]
    fn test_check_index_file_detects_size_mismatch() {
        let data = idx_bytes(&[(NeedleId(1), Offset::from_actual_offset(0), Size(50))]);
        let claimed = data.len() as i64 + 5;
        let (_count, errs) = check_index_file(&mut Cursor::new(data), claimed, Version(3));
        assert!(
            errs.iter().any(|e| e.contains("expected an index file")),
            "{:?}",
            errs
        );
    }

    #[test]
    fn test_write_index_entry() {
        let mut buf = Vec::new();
        write_index_entry(
            &mut buf,
            NeedleId(42),
            Offset::from_actual_offset(8 * 10),
            Size(512),
        )
        .unwrap();
        assert_eq!(buf.len(), NEEDLE_MAP_ENTRY_SIZE);

        let (key, offset, size) = idx_entry_from_bytes(&buf);
        assert_eq!(key, NeedleId(42));
        assert_eq!(offset.to_actual_offset(), 80);
        assert_eq!(size, Size(512));
    }
}
