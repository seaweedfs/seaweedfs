//! Set-union merge of `.ecj` deletion journals for EC shard copy / index
//! recovery. Mirrors Go's `weed/storage/erasure_coding/ecj_merge.go`.
//!
//! An EC volume's deletion journal (`<vid>.ecj`) is a *set* of deleted needle
//! ids stored as 8-byte big-endian records. Shard copy and index recovery fold
//! a peer's journal into the local one; they must append only the ids the
//! local journal lacks, or every `ec_balance` round trip doubles the file.
//!
//! The journal is only ever appended to, never replaced: a mounted `EcVolume`
//! holds it open, and a rename would leave that handle writing to an unlinked
//! inode, losing every later delete at the next mount. A mounted volume merges
//! through [`EcVolume::merge_journal`](super::ec_volume::EcVolume::merge_journal);
//! [`append_ecj_ids`] is for a journal no volume has open.

use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::storage::types::{NEEDLE_ID_SIZE, NeedleId};
use crate::storage::volume::fsync_dir;
use crate::storage::volume_open::open_volume_file;

/// Bytes per read when scanning a journal; a multiple of `NEEDLE_ID_SIZE`.
const ECJ_READ_CHUNK_BYTES: usize = 1 << 20;

/// Decodes `.ecj` records from a byte stream split at arbitrary boundaries (a
/// CopyFile stream, chunked reads), collecting the distinct ids. Memory
/// follows the number of distinct ids, not the journal's length. A trailing
/// partial record is never decoded.
#[derive(Default)]
pub(crate) struct EcjIdDecoder {
    ids: HashSet<NeedleId>,
    partial: [u8; NEEDLE_ID_SIZE],
    pending: usize,
}

impl EcjIdDecoder {
    pub(crate) fn push(&mut self, mut bytes: &[u8]) {
        if self.pending > 0 {
            let n = (NEEDLE_ID_SIZE - self.pending).min(bytes.len());
            self.partial[self.pending..self.pending + n].copy_from_slice(&bytes[..n]);
            self.pending += n;
            bytes = &bytes[n..];
            if self.pending < NEEDLE_ID_SIZE {
                return;
            }
            self.ids.insert(NeedleId::from_bytes(&self.partial));
            self.pending = 0;
        }
        let mut records = bytes.chunks_exact(NEEDLE_ID_SIZE);
        for record in &mut records {
            self.ids.insert(NeedleId::from_bytes(record));
        }
        let rest = records.remainder();
        self.partial[..rest.len()].copy_from_slice(rest);
        self.pending = rest.len();
    }

    pub(crate) fn into_ids(self) -> HashSet<NeedleId> {
        self.ids
    }
}

/// Read the distinct ids of the journal at `path` in bounded chunks. A missing
/// file reads as empty. Also returns the whole-record length read; a torn
/// trailing partial record is excluded from it.
pub(crate) fn read_ecj_ids(path: &str) -> io::Result<(HashSet<NeedleId>, u64)> {
    let mut file = match fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok((HashSet::new(), 0)),
        Err(e) => return Err(e),
    };
    let len = file.metadata()?.len();
    let size = len - len % NEEDLE_ID_SIZE as u64;
    let mut decoder = EcjIdDecoder::default();
    let mut buf = vec![0u8; (ECJ_READ_CHUNK_BYTES as u64).min(size) as usize];
    let mut off = 0u64;
    while off < size {
        let want = (buf.len() as u64).min(size - off) as usize;
        file.read_exact(&mut buf[..want])?;
        decoder.push(&buf[..want]);
        off += want as u64;
    }
    Ok((decoder.into_ids(), size))
}

/// The ids of `incoming` that `has` does not report, ascending, so a merge
/// appends deterministic output.
pub(crate) fn ecj_delta(
    incoming: &HashSet<NeedleId>,
    has: impl Fn(&NeedleId) -> bool,
) -> Vec<NeedleId> {
    let mut delta: Vec<NeedleId> = incoming.iter().copied().filter(|id| !has(id)).collect();
    delta.sort_unstable();
    delta
}

pub(crate) fn encode_ecj_ids(ids: &[NeedleId]) -> Vec<u8> {
    let mut buf = vec![0u8; ids.len() * NEEDLE_ID_SIZE];
    for (record, id) in buf.chunks_exact_mut(NEEDLE_ID_SIZE).zip(ids) {
        id.to_bytes(record);
    }
    buf
}

/// Append to the journal at `path` the ids of `incoming` that `local` lacks,
/// in one write and one fsync, returning how many were added. `local` and
/// `size` come from [`read_ecj_ids`] on the same path: if the journal's
/// whole-record length is no longer `size`, returns `Ok(None)` so the caller
/// re-reads. A torn tail past `size` is truncated first so the new records
/// stay aligned.
pub(crate) fn append_ecj_ids(
    path: &str,
    local: &HashSet<NeedleId>,
    incoming: &HashSet<NeedleId>,
    size: u64,
) -> io::Result<Option<usize>> {
    let delta = ecj_delta(incoming, |id| local.contains(id));
    if delta.is_empty() {
        return Ok(Some(0));
    }
    let created = !Path::new(path).exists();
    let mut file = open_volume_file(OpenOptions::new().read(true).write(true).create(true), path)?;
    let len = file.metadata()?.len();
    if len - len % NEEDLE_ID_SIZE as u64 != size {
        return Ok(None);
    }
    if len != size {
        file.set_len(size)?;
    }
    let appended = file
        .seek(SeekFrom::Start(size))
        .and_then(|_| file.write_all(&encode_ecj_ids(&delta)))
        .and_then(|_| file.sync_all());
    if let Err(e) = appended {
        let _ = file.set_len(size);
        return Err(e);
    }
    if created {
        fsync_dir(path)?;
    }
    Ok(Some(delta.len()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(v: &[u64]) -> HashSet<NeedleId> {
        v.iter().map(|&id| NeedleId(id)).collect()
    }

    fn bytes(v: &[u64]) -> Vec<u8> {
        encode_ecj_ids(&v.iter().map(|&id| NeedleId(id)).collect::<Vec<_>>())
    }

    fn records(path: &str) -> Vec<u64> {
        let data = fs::read(path).expect("read ecj");
        assert_eq!(data.len() % NEEDLE_ID_SIZE, 0, "journal must stay aligned");
        data.chunks_exact(NEEDLE_ID_SIZE)
            .map(|c| NeedleId::from_bytes(c).0)
            .collect()
    }

    /// Run the unmounted merge the way the server does: read, then append.
    fn merge_file(path: &str, incoming: &HashSet<NeedleId>) -> usize {
        let (local, size) = read_ecj_ids(path).expect("read");
        append_ecj_ids(path, &local, incoming, size)
            .expect("append")
            .expect("journal unchanged")
    }

    #[test]
    fn decoder_handles_records_split_across_chunks() {
        let mut stream = bytes(&[1, 2, 3, 2, 0x0102030405060708]);
        stream.extend_from_slice(&[9, 9, 9]);
        for chunk in 1..=stream.len() {
            let mut d = EcjIdDecoder::default();
            for piece in stream.chunks(chunk) {
                d.push(piece);
            }
            assert_eq!(
                d.into_ids(),
                ids(&[1, 2, 3, 0x0102030405060708]),
                "chunk {}",
                chunk
            );
        }
    }

    #[test]
    fn read_ecj_ids_dedups_and_ignores_torn_tail() {
        let dir = tempfile::tempdir().expect("tempdir");
        let missing = dir.path().join("missing.ecj");
        let (got, size) = read_ecj_ids(missing.to_str().unwrap()).expect("read");
        assert!(got.is_empty());
        assert_eq!(size, 0);

        let torn = dir.path().join("torn.ecj");
        let mut data = bytes(&[1, 2, 1]);
        data.extend_from_slice(&[7, 7, 7]);
        fs::write(&torn, data).unwrap();
        let (got, size) = read_ecj_ids(torn.to_str().unwrap()).expect("read");
        assert_eq!(got, ids(&[1, 2]));
        assert_eq!(size, 3 * NEEDLE_ID_SIZE as u64);

        // A bloated journal repeating a few ids across several read chunks
        // keeps only the distinct ids.
        let bloated = dir.path().join("bloated.ecj");
        let data: Vec<u8> = (0..300_000u64).flat_map(|i| bytes(&[i % 3])).collect();
        fs::write(&bloated, &data).unwrap();
        let (got, size) = read_ecj_ids(bloated.to_str().unwrap()).expect("read");
        assert_eq!(got, ids(&[0, 1, 2]));
        assert_eq!(size, data.len() as u64);
    }

    #[test]
    fn appends_only_missing_ids() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("vol.ecj");
        let path = path.to_str().unwrap();
        fs::write(path, bytes(&[1, 2, 3])).unwrap();
        assert_eq!(merge_file(path, &ids(&[3, 4])), 1);
        assert_eq!(records(path), vec![1, 2, 3, 4]);
    }

    #[test]
    fn round_trip_stays_constant() {
        // A->B->A->B 20 times: the old append path doubled the journal each trip.
        let dir = tempfile::tempdir().expect("tempdir");
        let a = dir.path().join("a.ecj");
        let b = dir.path().join("b.ecj");
        let (a, b) = (a.to_str().unwrap(), b.to_str().unwrap());
        fs::write(a, bytes(&[1, 2])).unwrap();
        fs::write(b, bytes(&[2, 3])).unwrap();
        for i in 0..20 {
            let (src, dst) = if i % 2 == 0 { (a, b) } else { (b, a) };
            let (incoming, _) = read_ecj_ids(src).expect("read");
            merge_file(dst, &incoming);
        }
        for path in [a, b] {
            let mut got = records(path);
            got.sort_unstable();
            assert_eq!(got, vec![1, 2, 3]);
        }
    }

    #[test]
    fn nothing_new_leaves_journal_alone() {
        let dir = tempfile::tempdir().expect("tempdir");
        let missing = dir.path().join("missing.ecj");
        assert_eq!(merge_file(missing.to_str().unwrap(), &ids(&[])), 0);
        assert!(
            !missing.exists(),
            "an empty merge must not create a journal"
        );

        let path = dir.path().join("vol.ecj");
        let path = path.to_str().unwrap();
        fs::write(path, bytes(&[1, 2])).unwrap();
        assert_eq!(merge_file(path, &ids(&[2, 1])), 0);
        assert_eq!(records(path), vec![1, 2]);
    }

    #[test]
    fn repairs_torn_tail() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("vol.ecj");
        let path = path.to_str().unwrap();
        let mut data = bytes(&[1, 2]);
        data.extend_from_slice(&[9, 9, 9]);
        fs::write(path, data).unwrap();
        assert_eq!(merge_file(path, &ids(&[3])), 1);
        assert_eq!(records(path), vec![1, 2, 3]);
    }

    #[test]
    fn rejects_changed_journal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("vol.ecj");
        let path = path.to_str().unwrap();
        fs::write(path, bytes(&[1])).unwrap();
        let (local, size) = read_ecj_ids(path).expect("read");
        fs::write(path, bytes(&[1, 5])).unwrap();
        let outcome = append_ecj_ids(path, &local, &ids(&[2]), size).expect("append");
        assert_eq!(outcome, None);
        assert_eq!(records(path), vec![1, 5]);
    }
}
