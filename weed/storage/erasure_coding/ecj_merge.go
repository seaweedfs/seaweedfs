package erasure_coding

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// An EC volume's deletion journal (<vid>.ecj) is a *set* of deleted needle ids
// stored as 8-byte big-endian records. Shard copy and index recovery fold a
// peer's journal into the local one; they must append only the ids the local
// journal lacks, or every ec_balance round trip doubles the file.
//
// The journal is only ever appended to, never replaced: a mounted EcVolume
// holds it open, and a rename would leave that handle writing to an unlinked
// inode, losing every later delete at the next mount.

// ErrEcjChanged reports that a journal's length moved between the read that
// computed a merge delta and the append that would publish it.
var ErrEcjChanged = errors.New("ec journal changed during merge")

// EcjIdDecoder decodes .ecj records from a byte stream split at arbitrary
// boundaries (a CopyFile stream, chunked reads), collecting the distinct ids.
// Memory follows the number of distinct ids, not the journal's length. A
// trailing partial record is never decoded.
type EcjIdDecoder struct {
	ids     map[types.NeedleId]struct{}
	partial [types.NeedleIdSize]byte
	pending int
}

func NewEcjIdDecoder() *EcjIdDecoder {
	return &EcjIdDecoder{ids: make(map[types.NeedleId]struct{})}
}

func (d *EcjIdDecoder) Write(p []byte) {
	if d.pending > 0 {
		n := copy(d.partial[d.pending:], p)
		d.pending += n
		p = p[n:]
		if d.pending < types.NeedleIdSize {
			return
		}
		d.ids[types.BytesToNeedleId(d.partial[:])] = struct{}{}
		d.pending = 0
	}
	whole := len(p) - len(p)%types.NeedleIdSize
	for i := 0; i < whole; i += types.NeedleIdSize {
		d.ids[types.BytesToNeedleId(p[i:i+types.NeedleIdSize])] = struct{}{}
	}
	d.pending = copy(d.partial[:], p[whole:])
}

func (d *EcjIdDecoder) Ids() map[types.NeedleId]struct{} {
	return d.ids
}

// ReadEcjIds reads the distinct ids of the journal at path in bounded chunks.
// A missing file reads as empty. size is the whole-record length read; a torn
// trailing partial record is excluded from it.
func ReadEcjIds(path string) (ids map[types.NeedleId]struct{}, size int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[types.NeedleId]struct{}), 0, nil
		}
		return nil, 0, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	size = fi.Size() - fi.Size()%int64(types.NeedleIdSize)
	d := NewEcjIdDecoder()
	buf := make([]byte, min(int64(ecjLoadChunkBytes), size))
	for off := int64(0); off < size; {
		want := min(int64(len(buf)), size-off)
		if _, err := f.ReadAt(buf[:want], off); err != nil {
			return nil, 0, fmt.Errorf("read %s at %d: %w", path, off, err)
		}
		d.Write(buf[:want])
		off += want
	}
	return d.Ids(), size, nil
}

// ecjDelta returns the ids of incoming that has does not report, ascending,
// so a merge appends deterministic output.
func ecjDelta(incoming map[types.NeedleId]struct{}, has func(types.NeedleId) bool) []types.NeedleId {
	var delta []types.NeedleId
	for id := range incoming {
		if !has(id) {
			delta = append(delta, id)
		}
	}
	slices.Sort(delta)
	return delta
}

func encodeEcjIds(ids []types.NeedleId) []byte {
	b := make([]byte, len(ids)*types.NeedleIdSize)
	for i, id := range ids {
		types.NeedleIdToBytes(b[i*types.NeedleIdSize:], id)
	}
	return b
}

// AppendEcjIds appends to the journal at path the ids of incoming that local
// lacks, in one write and one fsync, rolling the write back if the fsync
// fails. It is WriteEcjIds followed by EcjAppend.Sync; see WriteEcjIds for the
// contract.
func AppendEcjIds(path string, local, incoming map[types.NeedleId]struct{}, size int64) (added int, err error) {
	a, err := WriteEcjIds(path, local, incoming, size)
	if a == nil || err != nil {
		return 0, err
	}
	if err := a.Sync(); err != nil {
		a.Rollback()
		return 0, err
	}
	a.Close()
	return a.Added, nil
}

// EcjAppend is a journal append WriteEcjIds wrote but did not sync. The owner
// must end it with Close (after a successful Sync) or Rollback.
type EcjAppend struct {
	Added   int
	f       *os.File
	path    string
	size    int64 // whole-record length before the append
	delta   []types.NeedleId
	created bool
}

// WriteEcjIds appends to the journal at path the ids of incoming that local
// lacks, in one write, without syncing; it returns nil when there is nothing
// to add. It is for a journal no EcVolume has open; a mounted volume merges
// through EcVolume.MergeJournal instead. local and size come from ReadEcjIds
// on the same path: if the journal's whole-record length is no longer size,
// it returns ErrEcjChanged so the caller re-reads. A torn tail past size is
// truncated first so the new records stay aligned.
func WriteEcjIds(path string, local, incoming map[types.NeedleId]struct{}, size int64) (*EcjAppend, error) {
	delta := ecjDelta(incoming, func(id types.NeedleId) bool {
		_, ok := local[id]
		return ok
	})
	if len(delta) == 0 {
		return nil, nil
	}
	_, statErr := os.Stat(path)
	created := os.IsNotExist(statErr)
	f, err := backend.OpenVolumeFile(path, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	a := &EcjAppend{Added: len(delta), f: f, path: path, size: size, delta: delta, created: created}
	fi, err := f.Stat()
	if err != nil {
		a.Close()
		return nil, fmt.Errorf("stat %s: %w", path, err)
	}
	if fi.Size()-fi.Size()%int64(types.NeedleIdSize) != size {
		a.Close()
		return nil, ErrEcjChanged
	}
	if fi.Size() != size {
		if err := f.Truncate(size); err != nil {
			a.Close()
			return nil, fmt.Errorf("truncate torn tail of %s: %w", path, err)
		}
	}
	if _, err := f.WriteAt(encodeEcjIds(delta), size); err != nil {
		_ = f.Truncate(size)
		a.Close()
		return nil, fmt.Errorf("append %s: %w", path, err)
	}
	return a, nil
}

// Sync makes the append durable, including the journal's directory entry if
// the append created it.
func (a *EcjAppend) Sync() error {
	if err := a.f.Sync(); err != nil {
		return fmt.Errorf("sync %s: %w", a.path, err)
	}
	if a.created {
		if err := util.FsyncDir(filepath.Dir(a.path)); err != nil {
			return fmt.Errorf("fsync dir for %s: %w", a.path, err)
		}
	}
	return nil
}

// Rollback removes the append, for a journal no EcVolume has open, and closes
// it. See RollbackUnsynced.
func (a *EcjAppend) Rollback() {
	a.RollbackUnsynced(nil)
	a.Close()
}

// RollbackUnsynced removes an append whose fsync failed and reports whether it
// did. holders are the mounted volumes that opened the journal since the write
// (none can predate it: the write happens only while none is mounted); each
// loaded the append's records, and they leave its in-memory set too, so disk
// and memory agree the merge did not happen and a retried merge appends and
// syncs them again. The caller must exclude new mounts and other unmounted
// merges into the path.
//
// Invariant: only the journal's actual length decides, never a holder's cached
// ecjFileSize, which another holder's appends leave stale. With every holder's
// ecjFileAccessLock held nothing can append, so if the length is still this
// append's end, its records are the tail and removing them loses nothing. If
// anything follows them, it changes nothing and returns false; the caller then
// keeps the records and makes them durable with Resync.
func (a *EcjAppend) RollbackUnsynced(holders []*EcVolume) bool {
	for _, ev := range holders {
		ev.ecjFileAccessLock.Lock()
		defer ev.ecjFileAccessLock.Unlock()
	}
	if fi, err := a.f.Stat(); err != nil || fi.Size() != a.end() {
		return false
	}
	if err := a.f.Truncate(a.size); err != nil {
		return false
	}
	for _, ev := range holders {
		if ev.ecjFile == nil {
			continue // closed: it serves nothing and journals nothing
		}
		ev.ecjFileSize = a.size
		// Each holder loaded exactly the ids read before the append, which
		// exclude the delta, plus the delta: every delta id came from it.
		ev.deletedNeedlesLock.Lock()
		for _, id := range a.delta {
			delete(ev.deletedNeedles, id)
		}
		ev.deletedNeedlesLock.Unlock()
	}
	return true
}

// Resync rewrites the append's records in place and syncs them, for records
// that later ones now follow and so cannot be removed. A bare second fsync
// would prove nothing: after a failed writeback the kernel may have dropped
// the pages or marked them clean and still report the next fsync as clean.
// Rewriting the same bytes dirties them again, so a successful fsync means
// they reached the disk. Writing outside the locks is safe: appends land past
// these records, and every other truncate (a holder's failed append, a
// mount's torn-tail repair) lands at or past their end, since each holder
// loaded at least that much.
func (a *EcjAppend) Resync() error {
	if fi, err := a.f.Stat(); err != nil {
		return fmt.Errorf("stat %s: %w", a.path, err)
	} else if fi.Size() < a.end() {
		return fmt.Errorf("%s shrank below the merged records", a.path)
	}
	if _, err := a.f.WriteAt(encodeEcjIds(a.delta), a.size); err != nil {
		return fmt.Errorf("rewrite %s: %w", a.path, err)
	}
	return a.Sync()
}

func (a *EcjAppend) end() int64 {
	return a.size + int64(len(a.delta)*types.NeedleIdSize)
}

// Close releases the journal, keeping whatever the append wrote.
func (a *EcjAppend) Close() {
	_ = a.f.Close()
}

// MergeJournal folds a peer's deletion journal into this mounted volume. Under
// ecjFileAccessLock it appends only the ids not already deleted here, in one
// write and one fsync, then publishes them into the in-memory set — the same
// commit order as DeleteNeedleFromEcx, which it serializes with. The live
// handle is appended to in place, so no delete can land in an orphaned file.
func (ev *EcVolume) MergeJournal(ids map[types.NeedleId]struct{}) (added int, err error) {
	ev.ecjFileAccessLock.Lock()
	defer ev.ecjFileAccessLock.Unlock()
	if ev.ecjFile == nil {
		return 0, fmt.Errorf("ec volume %d closed", ev.VolumeId)
	}
	delta := ecjDelta(ids, ev.IsNeedleDeleted)
	if len(delta) == 0 {
		return 0, nil
	}
	if err := ev.appendJournalLocked(encodeEcjIds(delta)); err != nil {
		return 0, err
	}
	ev.deletedNeedlesLock.Lock()
	for _, id := range delta {
		ev.deletedNeedles[id] = struct{}{}
	}
	ev.deletedNeedlesLock.Unlock()
	return len(delta), nil
}
