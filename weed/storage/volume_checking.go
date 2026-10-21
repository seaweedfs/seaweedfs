package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// openIndex returns a file descriptor for the volume's index, and the index size in bytes.
func (v *Volume) openIndex() (*os.File, int64, error) {
	idxFileName := v.FileName(".idx")
	idxFile, err := backend.OpenVolumeFile(idxFileName, os.O_RDONLY)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open IDX file %s for volume %v: %v", idxFileName, v.Id, err)
	}

	idxStat, err := idxFile.Stat()
	if err != nil {
		idxFile.Close()
		return nil, 0, fmt.Errorf("failed to stat IDX file %s for volume %v: %v", idxFileName, v.Id, err)
	}

	if idxStat.Size() == 0 {
		if v.DataBackend == nil {
			idxFile.Close()
			return nil, 0, fmt.Errorf("volume %v has no data backend", v.Id)
		}
		volumeFileSize, _, err := v.DataBackend.GetStat()
		if err != nil {
			idxFile.Close()
			return nil, 0, fmt.Errorf("failed to stat storage for zero-size IDX volume %v: %w", v.Id, err)
		}

		// account for pre-allocated volumes (f.ex. after running "volume.grow") without data, as these
		// are allowed to have zero-size indices.
		if volumeFileSize > int64(super_block.SuperBlockSize) {
			idxFile.Close()
			return nil, 0, fmt.Errorf("zero-size IDX file for volume %v with store size %d", v.Id, volumeFileSize)
		}
	}

	return idxFile, idxStat.Size(), nil
}

// ScrubIndex checks the volume's index for issues.
func (v *Volume) ScrubIndex() (int64, []error) {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()

	idxFile, idxFileSize, err := v.openIndex()
	if err != nil {
		return 0, []error{err}
	}
	defer idxFile.Close()

	return idx.CheckIndexFile(idxFile, idxFileSize, v.Version())
}

// scrubVolumeData checks a volume content + index for issues.
func (v *Volume) scrubVolumeData(idxFile *os.File, idxFileSize int64) (int64, []error) {
	if v.DataBackend == nil {
		return 0, []error{fmt.Errorf("volume %d has no data backend", v.Id)}
	}

	// full scrubbing means also scrubbing the index
	var count int64
	_, errs := idx.CheckIndexFile(idxFile, idxFileSize, v.Version())

	// read and check every indexed needle
	var totalRead int64
	version := v.Version()
	err := idx.WalkIndexFile(idxFile, 0, func(id types.NeedleId, offset types.Offset, size types.Size) error {
		count++
		// A remote-tier delete records an offset-0 tombstone with no physical .dat
		// bytes, so it must not contribute to totalRead.
		if offset.IsZero() && size.IsDeleted() {
			return nil
		}
		physicalSize := types.Size(size.Raw())
		// compute the actual size of the needle in disk, including needle header, body and alignment padding.
		actualSize := int64(needle.GetActualSize(physicalSize, version))

		n := needle.Needle{}
		if err := n.ReadData(v.DataBackend, offset.ToActualOffset(), physicalSize, version); err != nil {
			errs = append(errs, fmt.Errorf("failed to read needle %d on volume %d: %v", id, v.Id, err))
		} else if n.Id != id {
			// The data CRC does not cover the header: a damaged needle id still
			// reads clean while every live-needle lookup on this replica keeps
			// finding the index key here.
			errs = append(errs, fmt.Errorf("index key %v does not match needle's Id %v on volume %d", id, n.Id, v.Id))
		}

		totalRead += actualSize
		return nil
	})
	if err != nil {
		errs = append(errs, err)
	}

	// check total volume file size
	wantSize := totalRead + super_block.SuperBlockSize
	dataSize, _, err := v.DataBackend.GetStat()

	if err != nil {
		errs = append(errs, fmt.Errorf("failed to stat data file for volume %d: %v", v.Id, err))
	} else {
		if dataSize < wantSize {
			errs = append(errs, fmt.Errorf("data file for volume %d is smaller (%d) than the %d needles it contains (%d)", v.Id, dataSize, count, wantSize))
		} else if dataSize != wantSize {
			errs = append(errs, fmt.Errorf("data file size for volume %d (%d) doesn't match the size for %d needles read (%d)", v.Id, dataSize, count, wantSize))
		}
	}

	return count, errs
}

// Scrub checks the entire volume content for issues.
func (v *Volume) Scrub() (int64, []error) {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()

	idxFile, idxFileSize, err := v.openIndex()
	if err != nil {
		return 0, []error{err}
	}
	defer idxFile.Close()

	return v.scrubVolumeData(idxFile, idxFileSize)
}

func CheckVolumeDataIntegrity(v *Volume, indexFile *os.File) (lastAppendAtNs uint64, err error) {
	var indexSize int64
	if indexSize, err = verifyIndexFileIntegrity(indexFile); err != nil {
		return 0, fmt.Errorf("verifyIndexFileIntegrity %s failed: %v", indexFile.Name(), err)
	}
	if indexSize == 0 {
		return 0, nil
	}
	// The deeper-than-tail structural check (every (offset + actual size)
	// fits inside .dat) lives in volume.load(): it reads
	// MaximumNeedleEnd from the needle map after the load walk, so we don't
	// need a redundant linear scan of the .idx here.

	// Verify the needle physically last in the .dat (the highest offset) and
	// that the .dat ends exactly at it. The .idx is not always in .dat append
	// order: weed fix and other rebuilds could rewrite it sorted by key, which
	// puts the highest-key needle last instead of the .dat-tail needle. Picking
	// the last file-position entry there compares a mid-file needle's tail
	// against the full .dat size and falsely flips the volume read-only on every
	// load.
	tailEntryPos, err := findDatTailEntryOffset(v, indexFile, indexSize)
	if err != nil {
		return 0, fmt.Errorf("CheckVolumeDataIntegrity %s: %v", indexFile.Name(), err)
	}
	if tailEntryPos < 0 {
		// pre-allocated / all-zero index, nothing to verify against the .dat
		return 0, nil
	}

	// A crash between appending a needle to the .dat and appending its entry to
	// the .idx leaves a .dat tail no index entry covers. Re-index the complete
	// needles in it and cut an incomplete remainder before the check below,
	// which would otherwise load the volume read-only. Only a volume that is
	// about to load writable is repaired; a read-only one is left as it is.
	// A repair that fails for any other reason than an unexplained tail (a
	// read, write, truncate or sync error) fails the check, so the volume
	// loads read-only instead of writable on a repair that may not be on disk.
	if !v.noWriteOrDelete && !v.noWriteCanDelete {
		reindexed, _, err := repairUnindexedDatTail(v, indexFile, indexSize, tailEntryPos)
		if err != nil {
			if !errors.Is(err, errTailNotTorn) {
				return 0, fmt.Errorf("CheckVolumeDataIntegrity %s: repair unindexed .dat tail: %v", indexFile.Name(), err)
			}
			glog.Warningf("volume %d: unindexed .dat tail not repaired: %v", v.Id, err)
		}
		if reindexed > 0 {
			if indexSize, err = verifyIndexFileIntegrity(indexFile); err != nil {
				return 0, fmt.Errorf("verifyIndexFileIntegrity %s after re-indexing failed: %v", indexFile.Name(), err)
			}
			if tailEntryPos, err = findDatTailEntryOffset(v, indexFile, indexSize); err != nil {
				return 0, fmt.Errorf("CheckVolumeDataIntegrity %s: %v", indexFile.Name(), err)
			}
		}
	}

	if lastAppendAtNs, err = doCheckAndFixVolumeData(v, indexFile, tailEntryPos); err != nil {
		return lastAppendAtNs, fmt.Errorf("CheckVolumeDataIntegrity %s failed: %v", indexFile.Name(), err)
	}
	return lastAppendAtNs, nil
}

// findDatTailEntryOffset returns the .idx file position of the entry whose
// needle is physically last in the .dat (highest offset). The common case — an
// append-ordered .idx — is resolved in O(1): the last entry's on-disk end
// equals the .dat size. A key-sorted .idx (e.g. legacy weed fix output) falls
// back to a single linear scan for the maximum offset. Returns -1 when no entry
// has a non-zero offset (a pre-allocated / all-zero index).
func findDatTailEntryOffset(v *Volume, indexFile *os.File, indexSize int64) (int64, error) {
	// Fast path: an append-ordered .idx ends with the .dat-tail needle, so the
	// last entry's on-disk end matches the .dat size.
	if v.DataBackend != nil {
		if datSize, _, statErr := v.DataBackend.GetStat(); statErr == nil && datSize > 0 {
			lastPos := indexSize - types.NeedleMapEntrySize
			entryBytes, err := readIndexEntryAtOffset(indexFile, lastPos)
			if err != nil {
				return 0, fmt.Errorf("read last index entry: %v", err)
			}
			_, offset, size := idx.IdxFileEntry(entryBytes)
			if !offset.IsZero() && needleDiskEnd(offset, size, v.Version()) == datSize {
				return lastPos, nil
			}
		}
	}

	// Slow path: scan the whole .idx for the entry at the maximum offset.
	maxEntryPos := int64(-1)
	maxActualOffset := int64(-1)
	entryPos := int64(0)
	if err := idx.WalkIndexFile(indexFile, 0, func(_ types.NeedleId, offset types.Offset, _ types.Size) error {
		if !offset.IsZero() {
			if ao := offset.ToActualOffset(); ao > maxActualOffset {
				maxActualOffset = ao
				maxEntryPos = entryPos
			}
		}
		entryPos += types.NeedleMapEntrySize
		return nil
	}); err != nil {
		return 0, fmt.Errorf("scan index: %v", err)
	}
	return maxEntryPos, nil
}

// needleDiskEnd returns the byte offset just past the needle's on-disk record.
func needleDiskEnd(offset types.Offset, size types.Size, version needle.Version) int64 {
	return offset.ToActualOffset() + needle.GetActualSize(types.Size(size.Raw()), version)
}

// errTailNotTorn marks a .dat tail that is not the result of an interrupted
// append; the volume keeps loading read-only.
var errTailNotTorn = errors.New("not a torn tail")

// repairUnindexedDatTail re-indexes the complete needles a crash left past the
// last indexed needle of a local version 3 volume, cuts an incomplete
// remainder, and returns how many needles it indexed and how many bytes it cut.
//
// A write appends the needle to the .dat, then its entry to the .idx, then
// answers the client. A process killed between the first two steps leaves a
// needle, or part of one, past the end of the needle at the highest indexed
// offset. After a power loss the order on disk is whatever the page cache
// flushed: a complete needle can survive whose .idx entry did not, although
// the client was answered. So a complete needle is never dropped: every
// complete needle in the tail (its header, body, checksum, timestamp and
// padding inside the file, its CRC valid, its append timestamp later than the
// needle before it) gets the .idx entry the volume server would have written,
// in .dat order, and the .idx is synced. A size 0 record is indexed at size 0,
// the same live entry the write path publishes for a write with no data. A
// delete appends the identical record, so a recovered size 0 record can also
// be a delete whose key then reads back as an empty object rather than
// not-found -- the two cannot be told apart, and resurrecting an empty
// placeholder beats a wrong tombstone, which drops an acknowledged write's
// object entirely. Only what follows the last complete
// needle and is itself incomplete is cut: fewer bytes than a header, a record
// running past the end of the file, or a last record whose bytes are damaged
// (its CRC fails or its body does not parse); the .dat is synced after the
// cut. A record that cannot be read is not cut: a read error is returned and
// nothing is changed.
//
// Nothing is changed when the tail is anything else: the .idx entry at the
// highest offset does not describe the needle there (id, size, CRC), a record
// inside the tail is damaged while more data follows it, a header carries id 0
// or a negative size, append timestamps go backwards, the .dat is beyond what
// 4-byte offsets can address, the needle version is not 3, or the .idx size is
// not a whole number of entries. The integrity check that follows then marks
// the volume read-only exactly as before.
func repairUnindexedDatTail(v *Volume, indexFile *os.File, indexSize int64, tailEntryPos int64) (int, int64, error) {
	version := v.Version()
	if version != needle.Version3 || v.DataBackend == nil {
		return 0, 0, nil
	}
	datSize, _, err := v.DataBackend.GetStat()
	if err != nil {
		return 0, 0, fmt.Errorf("stat %s: %v", v.FileName(".dat"), err)
	}
	if datSize <= int64(super_block.SuperBlockSize) || datSize > int64(types.MaxPossibleVolumeSize) {
		// past MaxPossibleVolumeSize the .idx offsets wrap: an entry's offset
		// does not name its needle's position on its own
		return 0, 0, nil
	}
	entry, err := readIndexEntryAtOffset(indexFile, tailEntryPos)
	if err != nil {
		return 0, 0, fmt.Errorf("read index entry at %d: %v", tailEntryPos, err)
	}
	key, offset, size := idx.IdxFileEntry(entry)
	if offset.IsZero() {
		return 0, 0, nil
	}
	start := needleDiskEnd(offset, size, version)
	if start >= datSize {
		// the .dat ends at the last needle (clean), or the index references
		// bytes the .dat does not have: not a torn tail, not ours to fix
		return 0, 0, nil
	}
	// The backend rounds its size up to the needle alignment; the walk needs
	// the bytes that are really there.
	fileSize := datSize
	if st, statErr := os.Stat(v.DataBackend.Name()); statErr == nil {
		fileSize = st.Size()
	}

	// Anchor: the needle at the highest indexed offset must be the one its
	// entry describes, whole and with a valid CRC.
	onDiskSize := size
	if size.IsDeleted() {
		onDiskSize = 0
	}
	anchor, err := readTailNeedle(v.DataBackend, version, offset.ToActualOffset(), onDiskSize)
	if err != nil {
		return 0, 0, fmt.Errorf("last indexed needle %v at %d: %v: %w", key, offset.ToActualOffset(), err, errTailNotTorn)
	}
	if anchor.Id != key {
		return 0, 0, fmt.Errorf("needle at %d is id %v, its index entry says id %v: %w", offset.ToActualOffset(), anchor.Id, key, errTailNotTorn)
	}

	entries, cutAt, err := walkUnindexedDatTail(v.DataBackend, version, start, fileSize, anchor.AppendAtNs)
	if err != nil {
		return 0, 0, err
	}
	reindexed := len(entries) / types.NeedleMapEntrySize
	if reindexed > 0 {
		if err := appendIndexEntries(indexFile, entries, indexSize); err != nil {
			return 0, 0, err
		}
	}
	var cut int64
	if cutAt < fileSize {
		if err := v.DataBackend.Truncate(cutAt); err != nil {
			return reindexed, 0, fmt.Errorf("truncate %s to %d: %v", v.FileName(".dat"), cutAt, err)
		}
		if err := v.DataBackend.Sync(); err != nil {
			return reindexed, fileSize - cutAt, fmt.Errorf("sync %s after truncating to %d: %v", v.FileName(".dat"), cutAt, err)
		}
		cut = fileSize - cutAt
	}
	glog.Warningf("volume %d: repaired the unindexed tail of %s: re-indexed %d needle(s), cut %d bytes (%d -> %d bytes)",
		v.Id, v.FileName(".dat"), reindexed, cut, fileSize, fileSize-cut)
	return reindexed, cut, nil
}

// indexAppender is the part of the .idx file the repair writes through.
type indexAppender interface {
	io.WriterAt
	Truncate(size int64) error
	Sync() error
	Name() string
}

// appendIndexEntries writes entries at the end (at) of the .idx and syncs it.
// A failed write can leave part of the entries behind, which would make the
// next load reject the .idx as a partial entry; they are removed again.
func appendIndexEntries(indexFile indexAppender, entries []byte, at int64) error {
	if _, err := indexFile.WriteAt(entries, at); err != nil {
		if truncErr := indexFile.Truncate(at); truncErr != nil {
			return fmt.Errorf("append %d bytes to %s: %v; truncate back to %d: %v", len(entries), indexFile.Name(), err, at, truncErr)
		}
		return fmt.Errorf("append %d bytes to %s: %v", len(entries), indexFile.Name(), err)
	}
	if err := indexFile.Sync(); err != nil {
		return fmt.Errorf("sync %s: %v", indexFile.Name(), err)
	}
	return nil
}

// readTailNeedle reads and verifies the whole record of a needle of the given
// on-disk size at offset: header, body, CRC and timestamp.
func readTailNeedle(datFile backend.BackendStorageFile, version needle.Version, at int64, size types.Size) (*needle.Needle, error) {
	blob, err := needle.ReadNeedleBlob(datFile, at, size, version)
	if err != nil {
		return nil, err
	}
	n := new(needle.Needle)
	if err := n.ReadBytes(blob, at, size, version); err != nil {
		return nil, err
	}
	if size > 0 && len(n.Data) == 0 {
		// A needle with no data is written with size 0, so a record with a
		// body whose data size reads 0 is damaged. The CRC covers only the
		// data, and ReadBytes does not check it for empty data.
		return nil, fmt.Errorf("needle %v at %d has size %d but no data: %w", n.Id, at, size, needle.ErrorCorrupted)
	}
	return n, nil
}

// walkUnindexedDatTail walks the needles from start to fileSize and returns
// the .idx entries of the complete ones, and the offset from which the rest is
// an incomplete last record (fileSize when there is none). It fails with
// errTailNotTorn when the tail is damaged in a way an interrupted append does
// not explain.
func walkUnindexedDatTail(datFile backend.BackendStorageFile, version needle.Version, start, fileSize int64, lastAppendAtNs uint64) ([]byte, int64, error) {
	var entries []byte
	at := start
	for at < fileSize {
		if fileSize-at < types.NeedleHeaderSize {
			return entries, at, nil // a partial header
		}
		header, _, _, err := needle.ReadNeedleHeader(datFile, version, at)
		if err != nil || header == nil {
			return nil, 0, fmt.Errorf("read needle header at %d: %v", at, err)
		}
		if header.Id == 0 || header.Size < 0 {
			return nil, 0, fmt.Errorf("record at %d has id %v size %d: %w", at, header.Id, header.Size, errTailNotTorn)
		}
		end := at + needle.GetActualSize(header.Size, version)
		if end > fileSize {
			return entries, at, nil // the last record runs past the end of the file
		}
		n, err := readTailNeedle(datFile, version, at, header.Size)
		if err != nil {
			if !errors.Is(err, needle.ErrorCorrupted) {
				// a read error says nothing about the bytes: leave them
				return nil, 0, fmt.Errorf("read record %v at %d: %w", header.Id, at, err)
			}
			if end == fileSize {
				return entries, at, nil // the last record is damaged: it never completed
			}
			return nil, 0, fmt.Errorf("record %v at %d is damaged and %d bytes follow it: %v: %w", header.Id, at, fileSize-end, err, errTailNotTorn)
		}
		if n.AppendAtNs <= lastAppendAtNs {
			return nil, 0, fmt.Errorf("record %v at %d was appended at %d, not after %d: %w", n.Id, at, n.AppendAtNs, lastAppendAtNs, errTailNotTorn)
		}
		lastAppendAtNs = n.AppendAtNs
		// A size 0 record is a delete or an empty write; the two append the
		// identical record, so it is indexed at size 0 the way the write path
		// indexes an empty write.
		entries = append(entries, needle_map.ToBytes(n.Id, types.ToOffset(at), header.Size)...)
		at = end
	}
	return entries, fileSize, nil
}

func doCheckAndFixVolumeData(v *Volume, indexFile *os.File, indexOffset int64) (lastAppendAtNs uint64, err error) {
	var lastIdxEntry []byte
	if lastIdxEntry, err = readIndexEntryAtOffset(indexFile, indexOffset); err != nil {
		return 0, fmt.Errorf("readLastIndexEntry %s failed: %v", indexFile.Name(), err)
	}
	key, offset, size := idx.IdxFileEntry(lastIdxEntry)
	if offset.IsZero() {
		return 0, nil
	}
	if size < 0 {
		// Deletion tombstone: the .idx carries TombstoneFileSize (-1) but the
		// appended needle in .dat carries Size=0. verifyDeletedNeedleIntegrity
		// reads it with Size=0 (the size check / 32GB wrap-around retry would
		// otherwise read past EOF) and still verifies the .dat ends at it.
		if lastAppendAtNs, err = verifyDeletedNeedleIntegrity(v.DataBackend, v.Version(), offset.ToActualOffset(), key); err != nil {
			if err == io.EOF || err == ErrorSizeMismatch {
				return lastAppendAtNs, err
			}
			return lastAppendAtNs, fmt.Errorf("verifyDeletedNeedleIntegrity %s failed: %v", indexFile.Name(), err)
		}
	} else {
		if lastAppendAtNs, err = verifyNeedleIntegrity(v.DataBackend, v.Version(), offset.ToActualOffset(), key, size); err != nil {
			if err == ErrorSizeMismatch {
				return verifyNeedleIntegrity(v.DataBackend, v.Version(), offset.ToActualOffset()+int64(types.MaxPossibleVolumeSize), key, size)
			}
			return lastAppendAtNs, err
		}
	}
	return lastAppendAtNs, nil
}

// recoverLastModifiedTs points the TTL clock at the newest write recorded in
// the volume, replacing the .dat mtime the loader starts from. A delete appends
// a tombstone and vacuum rewrites the .dat wholesale, so the mtime moves
// without any write: every restart of a volume taking delete traffic re-armed
// expired() for another full TTL and the volume was never reclaimed. Left on
// the mtime when no write is recoverable.
func (v *Volume) recoverLastModifiedTs(indexFile *os.File) {
	if v.Ttl == nil || v.Ttl.Minutes() == 0 {
		return
	}
	indexSize, err := verifyIndexFileIntegrity(indexFile)
	if err != nil || indexSize == 0 {
		return
	}
	appendAtNs, key, err := findLastWriteAppendAtNs(v, indexFile, indexSize)
	if err != nil {
		glog.Warningf("volume %d recover last write from %s: %v", v.Id, indexFile.Name(), err)
		return
	}
	if appendAtNs == 0 {
		return
	}
	v.lastModifiedTsSeconds = appendAtNs / uint64(time.Second)
	if appendAtNs > v.lastWriteAppendAtNs {
		v.lastWriteAppendAtNs = appendAtNs
		v.lastWriteNeedleKey = key
		v.lastWriteDeleted = false
	}
}

// vacuumedLastWriteScanEntries bounds the work a vacuumed volume's recovery
// does, where key order makes every write a candidate for the newest one. A
// volume with more live needles than this keeps the .dat mtime: reading a
// subset could recover a timestamp older than the newest write and expire data
// still inside its TTL, so a scan that will not fit declines instead of
// guessing. A variable so tests can exercise that path.
var vacuumedLastWriteScanEntries = 1 << 16

// findLastWriteAppendAtNs scans the .idx backwards for the newest write -- an
// entry that is not a deletion tombstone -- and returns that needle's append
// timestamp and key. The .idx and the .dat share an order, so an
// append-ordered volume answers with the first write the scan reaches. Vacuum
// rewrites both in key order, which tracks write order only because the master
// issues keys increasing: an overwrite keeps its original, lower key, so a
// vacuumed volume has to take the maximum over every write it indexes. Returns
// 0 when the .idx holds nothing but tombstones, when a vacuumed volume holds
// more needles than the scan budget, or for a volume older than version 3,
// whose needles carry no append timestamp.
func findLastWriteAppendAtNs(v *Volume, indexFile *os.File, indexSize int64) (uint64, types.NeedleId, error) {
	version := v.Version()
	if version != needle.Version3 {
		return 0, 0, nil
	}
	scanEveryWrite := v.SuperBlock.CompactionRevision > 0
	entryBudget := vacuumedLastWriteScanEntries
	if scanEveryWrite && !affordableVacuumedScan(indexFile, indexSize, v.Id, v.FileName(".dat")) {
		return 0, 0, nil
	}
	var lastWriteAppendAtNs uint64
	var lastWriteKey types.NeedleId
	dead := make(map[types.NeedleId]struct{})
	block := make([]byte, types.NeedleMapEntrySize*idx.RowsToRead)
	for end := indexSize; end > 0; {
		start := max(end-int64(len(block)), 0)
		entries := block[:end-start]
		readCount, err := indexFile.ReadAt(entries, start)
		if err == io.EOF && readCount == len(entries) {
			err = nil
		}
		if err != nil {
			return 0, 0, fmt.Errorf("read %s at %d: %v", indexFile.Name(), start, err)
		}
		for i := len(entries) - types.NeedleMapEntrySize; i >= 0; i -= types.NeedleMapEntrySize {
			key, offset, size := idx.IdxFileEntry(entries[i : i+types.NeedleMapEntrySize])
			// The first row a key presents is its latest state: a tombstone
			// there retires the write rows beneath it.
			if _, gone := dead[key]; gone {
				continue
			}
			if offset.IsZero() || size.IsDeleted() {
				dead[key] = struct{}{}
				continue
			}
			needleOffset := findNeedleOffset(v.DataBackend, version, offset.ToActualOffset(), key, size)
			if needleOffset < 0 {
				continue
			}
			appendAtNs, err := readNeedleAppendAtNs(v.DataBackend, needleOffset, size)
			if err != nil {
				return 0, 0, err
			}
			if appendAtNs > lastWriteAppendAtNs {
				lastWriteAppendAtNs = appendAtNs
				lastWriteKey = key
			}
			if !scanEveryWrite {
				return lastWriteAppendAtNs, lastWriteKey, nil
			}
			if entryBudget--; entryBudget == 0 {
				glog.V(0).Infof("volume %d: more than %d needles to scan for its last write, keeping the %s mtime",
					v.Id, vacuumedLastWriteScanEntries, v.FileName(".dat"))
				return 0, 0, nil
			}
		}
		end = start
	}
	return lastWriteAppendAtNs, lastWriteKey, nil
}

// affordableVacuumedScan reports whether a vacuumed volume's recovery scan fits
// its budget. A vacuumed volume must take the maximum over every write it
// indexes, so a volume holding more live needles than vacuumedLastWriteScanEntries
// would spend up to two random .dat reads per needle and then give up, keeping
// the .dat mtime anyway. Counting the live .idx entries first is one short
// sequential pass over a small file, and it skips the scan before any .dat I/O
// when the scan provably cannot succeed.
func affordableVacuumedScan(indexFile *os.File, indexSize int64, volumeId needle.VolumeId, datFileName string) bool {
	liveEntries, err := countLiveIndexEntries(indexFile, indexSize, vacuumedLastWriteScanEntries)
	if err != nil {
		glog.Warningf("volume %d count live entries in %s: %v", volumeId, indexFile.Name(), err)
		return true
	}
	if liveEntries >= vacuumedLastWriteScanEntries {
		glog.V(0).Infof("volume %d: more than %d needles to scan for its last write, keeping the %s mtime",
			volumeId, vacuumedLastWriteScanEntries, datFileName)
		return false
	}
	return true
}

// countLiveIndexEntries counts the .idx entries describing a live needle,
// stopping early once the count passes limit. Tombstones and zero offsets are
// skipped exactly as findLastWriteAppendAtNs skips them.
func countLiveIndexEntries(indexFile *os.File, indexSize int64, limit int) (int, error) {
	count := 0
	block := make([]byte, types.NeedleMapEntrySize*idx.RowsToRead)
	for start := int64(0); start < indexSize; {
		end := start + int64(len(block))
		if end > indexSize {
			end = indexSize
		}
		entries := block[:end-start]
		readCount, err := indexFile.ReadAt(entries, start)
		if err == io.EOF && readCount == len(entries) {
			err = nil
		}
		if err != nil {
			return 0, fmt.Errorf("read %s at %d: %v", indexFile.Name(), start, err)
		}
		for i := 0; i+types.NeedleMapEntrySize <= len(entries); i += types.NeedleMapEntrySize {
			_, offset, size := idx.IdxFileEntry(entries[i : i+types.NeedleMapEntrySize])
			if offset.IsZero() || size.IsDeleted() {
				continue
			}
			if count++; count > limit {
				return count, nil
			}
		}
		start = end
	}
	return count, nil
}

// findNeedleOffset returns the .dat offset holding the needle an .idx entry
// describes, or -1 when no needle there matches it. A .dat past
// MaxPossibleVolumeSize wraps the 4-byte offsets in its .idx, so the needle can
// sit one volume size further in; doCheckAndFixVolumeData retries the same way.
func findNeedleOffset(datFile backend.BackendStorageFile, version needle.Version, offset int64, key types.NeedleId, size types.Size) int64 {
	for _, at := range []int64{offset, offset + int64(types.MaxPossibleVolumeSize)} {
		n, _, _, err := needle.ReadNeedleHeader(datFile, version, at)
		if err == nil && n.Id == key && n.Size == size {
			return at
		}
	}
	return -1
}

// readNeedleAppendAtNs reads the append timestamp a version 3 needle carries
// past its checksum.
func readNeedleAppendAtNs(datFile backend.BackendStorageFile, offset int64, size types.Size) (uint64, error) {
	bytes := make([]byte, types.TimestampSize)
	readCount, err := datFile.ReadAt(bytes, offset+types.NeedleHeaderSize+int64(size)+needle.NeedleChecksumSize)
	if err == io.EOF && readCount == types.TimestampSize {
		err = nil
	}
	if err != nil {
		return 0, err
	}
	return util.BytesToUint64(bytes), nil
}

func verifyIndexFileIntegrity(indexFile *os.File) (indexSize int64, err error) {
	if indexSize, err = util.GetFileSize(indexFile); err == nil {
		if indexSize%types.NeedleMapEntrySize != 0 {
			err = fmt.Errorf("index file's size is %d bytes, maybe corrupted", indexSize)
		}
	}
	return
}

func readIndexEntryAtOffset(indexFile *os.File, offset int64) (bytes []byte, err error) {
	if offset < 0 {
		err = fmt.Errorf("offset %d for index file is invalid", offset)
		return
	}
	bytes = make([]byte, types.NeedleMapEntrySize)
	var readCount int
	readCount, err = indexFile.ReadAt(bytes, offset)
	if err == io.EOF && readCount == types.NeedleMapEntrySize {
		err = nil
	}
	return
}

func verifyNeedleIntegrity(datFile backend.BackendStorageFile, v needle.Version, offset int64, key types.NeedleId, size types.Size) (lastAppendAtNs uint64, err error) {
	n, _, _, err := needle.ReadNeedleHeader(datFile, v, offset)
	if err == io.EOF {
		return 0, err
	}
	if err != nil {
		return 0, fmt.Errorf("read %s at %d", datFile.Name(), offset)
	}
	if n.Size != size {
		return 0, ErrorSizeMismatch
	}
	if v == needle.Version3 {
		n.AppendAtNs, err = readNeedleAppendAtNs(datFile, offset, size)
		if err == io.EOF {
			return 0, err
		}
		if err != nil {
			return 0, fmt.Errorf("verifyNeedleIntegrity check %s entry offset %d size %d: %v", datFile.Name(), offset, size, err)
		}
		fileTailOffset := offset + needle.GetActualSize(size, v)
		fileSize, _, err := datFile.GetStat()
		if err != nil {
			return 0, fmt.Errorf("stat file %s: %v", datFile.Name(), err)
		}
		if fileSize == fileTailOffset {
			return n.AppendAtNs, nil
		}
		if fileSize > fileTailOffset {
			glog.Warningf("data file %s actual %d bytes expected %d bytes!", datFile.Name(), fileSize, fileTailOffset)
			return n.AppendAtNs, fmt.Errorf("data file %s actual %d bytes expected %d bytes", datFile.Name(), fileSize, fileTailOffset)
		}
		glog.Warningf("data file %s has %d bytes, less than expected %d bytes!", datFile.Name(), fileSize, fileTailOffset)
	}
	if err = n.ReadData(datFile, offset, size, v); err != nil {
		return n.AppendAtNs, fmt.Errorf("read data [%d,%d) : %v", offset, offset+int64(size), err)
	}
	if n.Id != key {
		return n.AppendAtNs, fmt.Errorf("index key %v does not match needle's Id %v", key, n.Id)
	}
	return n.AppendAtNs, err
}

func verifyDeletedNeedleIntegrity(datFile backend.BackendStorageFile, v needle.Version, offset int64, key types.NeedleId) (lastAppendAtNs uint64, err error) {
	n := new(needle.Needle)
	// Tombstones are appended with DataSize=0, so the on-disk header carries
	// Size=0. TombstoneFileSize (-1) lives only in the .idx; passing it to
	// ReadData fails the size check and triggers the 32GB wrap-around retry,
	// which reads past EOF and falsely marks the volume read-only.
	size := types.Size(0)
	if err = n.ReadData(datFile, offset, size, v); err != nil {
		// A truncated tail surfaces here as io.EOF; pass it (and a size mismatch)
		// through so the volume is marked read-only.
		if err == io.EOF || err == ErrorSizeMismatch {
			return n.AppendAtNs, err
		}
		return n.AppendAtNs, fmt.Errorf("read data [%d,%d) : %v", offset, offset+needle.GetActualSize(size, v), err)
	}
	if n.Id != key {
		return n.AppendAtNs, fmt.Errorf("index key %v does not match needle's Id %v", key, n.Id)
	}
	// This tombstone is the needle at the .dat tail (the highest offset), so the
	// .dat must end exactly at it; extra bytes mean an unindexed trailing record.
	fileSize, _, statErr := datFile.GetStat()
	if statErr != nil {
		return n.AppendAtNs, fmt.Errorf("stat file %s: %v", datFile.Name(), statErr)
	}
	if fileTailOffset := offset + needle.GetActualSize(size, v); fileSize > fileTailOffset {
		glog.Warningf("data file %s actual %d bytes expected %d bytes!", datFile.Name(), fileSize, fileTailOffset)
		return n.AppendAtNs, fmt.Errorf("data file %s actual %d bytes expected %d bytes", datFile.Name(), fileSize, fileTailOffset)
	}
	return n.AppendAtNs, nil
}

func (v *Volume) checkIdxFile() error {
	datFileSize, _, err := v.DataBackend.GetStat()
	if err != nil {
		return fmt.Errorf("get stat %s: %v", v.FileName(".dat"), err)
	}
	if datFileSize <= super_block.SuperBlockSize {
		return nil
	}
	indexFileName := v.FileName(".idx")
	if util.FileExists(indexFileName) {
		return nil
	}
	return fmt.Errorf("idx file %s does not exists", indexFileName)
}
