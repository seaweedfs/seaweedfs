//! Streaming write buffer: stages dirty file bytes per chunk so a flush can
//! upload completed chunks incrementally and free them, keeping resident memory
//! bounded (a few chunks) regardless of file size — instead of holding the whole
//! file in RAM.
//!
//! Modelled on the SeaweedFS Go mount page writer (`weed/mount/page_writer`):
//! dirty bytes are keyed by chunk index, a fully-written chunk is "sealed" and
//! uploaded as an overlay `FileChunk` (newest wins by `modified_ts_ns` at read
//! time, see [`crate::read::resolve_visibles`]), and reads merge committed
//! chunks with the still-staged bytes.
//!
//! This type is pure (no I/O): the daemon drives the actual uploads. It tracks
//! *which* bytes are dirty (per-chunk written intervals) so that a partially
//! written chunk only ever uploads the bytes the caller actually wrote — never
//! zero-filling a gap, which would clobber underlying committed data.

use std::collections::BTreeMap;

/// Dirty bytes for one chunk index. `data` is dense from offset 0; only the
/// ranges listed in `intervals` (chunk-relative, merged, sorted, non-adjacent)
/// hold caller-written bytes.
struct StagedChunk {
    data: Vec<u8>,
    intervals: Vec<(usize, usize)>,
}

impl StagedChunk {
    fn new() -> Self {
        Self {
            data: Vec::new(),
            intervals: Vec::new(),
        }
    }

    /// Mark `[lo, hi)` written, coalescing with touching/overlapping intervals.
    fn mark(&mut self, lo: usize, hi: usize) {
        let mut merged = (lo, hi);
        let mut out: Vec<(usize, usize)> = Vec::with_capacity(self.intervals.len() + 1);
        for &(s, e) in &self.intervals {
            if e < merged.0 || s > merged.1 {
                out.push((s, e)); // disjoint (note: adjacent merges, hence >/<)
            } else {
                merged.0 = merged.0.min(s);
                merged.1 = merged.1.max(e);
            }
        }
        out.push(merged);
        out.sort_unstable();
        self.intervals = out;
    }

    /// True if `[0, n)` is covered by a single interval.
    fn covers(&self, n: usize) -> bool {
        matches!(self.intervals.first(), Some(&(0, e)) if e >= n) && self.intervals.len() == 1
    }
}

/// A contiguous run of staged bytes ready to upload as one overlay chunk.
pub struct SealPiece {
    /// Chunk index this run belongs to (for [`StreamingWriteBuffer::drop_chunk`]).
    pub chunk_index: u64,
    /// Absolute file offset of the run.
    pub offset: u64,
    /// The written bytes.
    pub data: Vec<u8>,
}

/// Per-file streaming write buffer. Stages dirty bytes by chunk index and tracks
/// the logical file size; the daemon seals chunks (uploads + [`drop_chunk`]s) to
/// keep resident memory bounded.
///
/// [`drop_chunk`]: StreamingWriteBuffer::drop_chunk
pub struct StreamingWriteBuffer {
    chunk_size: usize,
    staged: BTreeMap<u64, StagedChunk>,
    size: u64,
}

impl StreamingWriteBuffer {
    pub fn new(chunk_size: usize) -> Self {
        Self::with_size(chunk_size, 0)
    }

    /// Construct with an initial logical size — the existing size of a file being
    /// modified, so getattr/read report it before any write grows the buffer.
    pub fn with_size(chunk_size: usize, size: u64) -> Self {
        assert!(chunk_size > 0);
        Self {
            chunk_size,
            staged: BTreeMap::new(),
            size,
        }
    }

    /// Logical file size (max of all writes / truncates).
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Total bytes currently staged in memory (the resident-memory pressure).
    pub fn staged_len(&self) -> usize {
        self.staged.values().map(|c| c.data.len()).sum()
    }

    /// No staged bytes remain (everything sealed/uploaded).
    pub fn is_empty(&self) -> bool {
        self.staged.is_empty()
    }

    /// Stage `data` written at absolute `offset`, splitting across chunk
    /// boundaries. Grows the logical size to cover the write.
    pub fn write(&mut self, offset: u64, data: &[u8]) {
        if data.is_empty() {
            return;
        }
        let mut off = offset;
        let mut rest = data;
        let cs = self.chunk_size as u64;
        while !rest.is_empty() {
            let idx = off / cs;
            let chunk_start = idx * cs;
            let in_chunk = (off - chunk_start) as usize;
            let take = rest.len().min(self.chunk_size - in_chunk);
            let chunk = self.staged.entry(idx).or_insert_with(StagedChunk::new);
            let end = in_chunk + take;
            if chunk.data.len() < end {
                chunk.data.resize(end, 0);
            }
            chunk.data[in_chunk..end].copy_from_slice(&rest[..take]);
            chunk.mark(in_chunk, end);
            off += take as u64;
            rest = &rest[take..];
        }
        self.size = self.size.max(offset.saturating_add(data.len() as u64));
    }

    /// Resize the logical file to `new_size`. Shrinking drops staged bytes beyond
    /// the new end; growing leaves a hole (reads return zeros there).
    pub fn truncate(&mut self, new_size: u64) {
        if new_size < self.size {
            let cs = self.chunk_size as u64;
            self.staged.retain(|&idx, _| idx * cs < new_size);
            if let Some((&idx, chunk)) = self.staged.range_mut(..).next_back() {
                let chunk_start = idx * cs;
                if chunk_start < new_size && new_size < chunk_start + cs {
                    let keep = (new_size - chunk_start) as usize;
                    if chunk.data.len() > keep {
                        chunk.data.truncate(keep);
                    }
                    chunk.intervals.retain_mut(|iv| {
                        iv.1 = iv.1.min(keep);
                        iv.0 < iv.1
                    });
                }
            }
        }
        self.size = new_size;
    }

    /// Overlay still-staged bytes onto `out` (which the caller pre-filled with
    /// committed data) for the read window starting at absolute `offset`.
    pub fn overlay_read(&self, offset: u64, out: &mut [u8]) {
        let want_start = offset;
        let want_end = offset + out.len() as u64;
        let cs = self.chunk_size as u64;
        for (&idx, chunk) in &self.staged {
            let chunk_start = idx * cs;
            if chunk_start >= want_end || chunk_start + cs <= want_start {
                continue;
            }
            for &(lo, hi) in &chunk.intervals {
                let abs_lo = chunk_start + lo as u64;
                let abs_hi = chunk_start + hi as u64;
                let s = abs_lo.max(want_start);
                let e = abs_hi.min(want_end);
                if s < e {
                    let dst = (s - want_start) as usize..(e - want_start) as usize;
                    let src = (s - chunk_start) as usize..(e - chunk_start) as usize;
                    out[dst].copy_from_slice(&chunk.data[src]);
                }
            }
        }
    }

    /// Indices of chunks fully covering `[0, chunk_size)` — safe to seal eagerly
    /// while writing so memory stays bounded. The final short chunk is excluded
    /// (it only seals at flush via [`drain_all`]).
    ///
    /// [`drain_all`]: StreamingWriteBuffer::drain_all
    pub fn full_chunk_indices(&self) -> Vec<u64> {
        self.staged
            .iter()
            .filter(|(_, c)| c.covers(self.chunk_size))
            .map(|(&idx, _)| idx)
            .collect()
    }

    /// The lowest staged chunk index (for pressure-sealing a partial chunk).
    pub fn lowest_index(&self) -> Option<u64> {
        self.staged.keys().next().copied()
    }

    /// Every staged chunk index (used on flush to seal all that remain).
    pub fn staged_indices(&self) -> Vec<u64> {
        self.staged.keys().copied().collect()
    }

    /// Upload pieces for one chunk: one [`SealPiece`] per contiguous written
    /// interval (so gaps are never zero-filled). Does NOT remove the chunk —
    /// call [`drop_chunk`] once the uploads are confirmed, so a concurrent read
    /// still sees the bytes while they are in flight.
    ///
    /// [`drop_chunk`]: StreamingWriteBuffer::drop_chunk
    pub fn pieces_for(&self, idx: u64) -> Vec<SealPiece> {
        let cs = self.chunk_size as u64;
        let chunk_start = idx * cs;
        match self.staged.get(&idx) {
            Some(chunk) => chunk
                .intervals
                .iter()
                .map(|&(lo, hi)| SealPiece {
                    chunk_index: idx,
                    offset: chunk_start + lo as u64,
                    data: chunk.data[lo..hi].to_vec(),
                })
                .collect(),
            None => Vec::new(),
        }
    }

    /// Pieces for every staged chunk (used on flush to seal the partial tail).
    pub fn drain_all(&self) -> Vec<SealPiece> {
        self.staged
            .keys()
            .flat_map(|&idx| self.pieces_for(idx))
            .collect()
    }

    /// Forget a chunk's staged bytes once its pieces have been uploaded.
    pub fn drop_chunk(&mut self, idx: u64) {
        self.staged.remove(&idx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CS: usize = 4 * 1024 * 1024;

    #[test]
    fn sequential_fills_seal_eagerly() {
        let mut b = StreamingWriteBuffer::new(CS);
        // Three full chunks written in 1 MiB pieces, then a short tail.
        for i in 0..(3 * 4) {
            b.write(i as u64 * (1 << 20), &vec![1u8; 1 << 20]);
        }
        b.write(3 * CS as u64, &vec![2u8; 1000]);
        assert_eq!(b.size(), 3 * CS as u64 + 1000);
        // Chunks 0,1,2 are full; the tail (chunk 3) is not.
        assert_eq!(b.full_chunk_indices(), vec![0, 1, 2]);
    }

    #[test]
    fn full_chunk_pieces_are_single_run() {
        let mut b = StreamingWriteBuffer::new(CS);
        b.write(0, &vec![7u8; CS]);
        let pieces = b.pieces_for(0);
        assert_eq!(pieces.len(), 1);
        assert_eq!(pieces[0].offset, 0);
        assert_eq!(pieces[0].data.len(), CS);
    }

    #[test]
    fn sparse_chunk_uploads_only_written_intervals() {
        let mut b = StreamingWriteBuffer::new(CS);
        // Two disjoint runs in chunk 0, with a gap between them.
        b.write(0, &[0xaa; 100]);
        b.write(1_000_000, &[0xbb; 50]);
        assert!(b.full_chunk_indices().is_empty()); // not fully covered
        let pieces = b.pieces_for(0);
        assert_eq!(pieces.len(), 2);
        assert_eq!((pieces[0].offset, pieces[0].data.len()), (0, 100));
        assert_eq!((pieces[1].offset, pieces[1].data.len()), (1_000_000, 50));
    }

    #[test]
    fn an_empty_write_past_the_end_keeps_the_size() {
        let mut b = StreamingWriteBuffer::with_size(CS, 10);
        b.write(1000, &[]);
        assert_eq!(b.size(), 10);
        assert!(b.is_empty());
    }

    #[test]
    fn touching_writes_coalesce() {
        let mut b = StreamingWriteBuffer::new(CS);
        b.write(0, &[1u8; 100]);
        b.write(100, &[1u8; 100]); // adjacent -> one interval
        assert_eq!(b.pieces_for(0).len(), 1);
        assert_eq!(b.pieces_for(0)[0].data.len(), 200);
    }

    #[test]
    fn overlay_read_overwrites_committed() {
        let mut b = StreamingWriteBuffer::new(CS);
        b.write(10, &[0xfe; 4]);
        let mut out = vec![0u8; 20]; // pretend committed data is all zeros
        b.overlay_read(0, &mut out);
        let mut want = vec![0u8; 20];
        want[10..14].fill(0xfe);
        assert_eq!(out, want);
    }

    #[test]
    fn overlay_read_partial_window() {
        let mut b = StreamingWriteBuffer::new(CS);
        b.write(CS as u64 - 2, &[9u8; 4]); // straddles chunk boundary
        let mut out = vec![0u8; 4];
        b.overlay_read(CS as u64 - 2, &mut out);
        assert_eq!(out, vec![9u8; 4]);
    }

    #[test]
    fn truncate_shrinks_and_drops() {
        let mut b = StreamingWriteBuffer::new(CS);
        b.write(0, &vec![1u8; CS + 100]); // chunk 0 full + 100 into chunk 1
        b.truncate(CS as u64 + 10);
        assert_eq!(b.size(), CS as u64 + 10);
        let p = b.pieces_for(1);
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].data.len(), 10);
        b.truncate(5);
        assert!(b.pieces_for(1).is_empty());
        assert_eq!(b.pieces_for(0)[0].data.len(), 5);
    }

    #[test]
    fn drop_chunk_frees_memory() {
        let mut b = StreamingWriteBuffer::new(CS);
        b.write(0, &vec![1u8; CS]);
        b.write(CS as u64, &vec![1u8; 1000]);
        assert_eq!(b.staged_len(), CS + 1000);
        b.drop_chunk(0);
        assert_eq!(b.staged_len(), 1000);
        assert_eq!(b.lowest_index(), Some(1));
    }
}
