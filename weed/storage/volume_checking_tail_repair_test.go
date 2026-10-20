package storage

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// A torn tail is what a crash between the two appends of a write leaves: the
// needle is in the .dat, its entry never reached the .idx. After a process
// crash the client was never answered; after a power loss it may have been.
// These tests write needles (and deletes) normally, drop the last .idx
// entries, cut or damage the .dat as a crash would, and reload the volume
// through the real load path.

type tornOp struct {
	id  uint64
	del bool
}

type tornFixture struct {
	dir     string
	datName string
	idxName string
	starts  []int64 // .dat offset of each op's record
	end     int64   // .dat size after every op
	written map[uint64]*needle.Needle
}

func newTornTailTestVolume(t *testing.T, dir string) *Volume {
	t.Helper()
	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("load volume: %v", err)
	}
	return v
}

// buildTornFixture runs ops against a fresh volume (each op appends one .dat
// record and one .idx entry), closes it and drops the last unindexed entries.
func buildTornFixture(t *testing.T, ops []tornOp, unindexed int) *tornFixture {
	t.Helper()
	f := &tornFixture{dir: t.TempDir(), written: map[uint64]*needle.Needle{}}
	v := newTornTailTestVolume(t, f.dir)
	for _, op := range ops {
		at, _, err := v.DataBackend.GetStat()
		if err != nil {
			t.Fatal(err)
		}
		f.starts = append(f.starts, at)
		if op.del {
			if _, err := v.doDeleteRequest(newEmptyNeedle(op.id)); err != nil {
				t.Fatalf("delete needle %d: %v", op.id, err)
			}
			continue
		}
		n := newRandomNeedle(op.id)
		if _, _, _, err := v.writeNeedle2(n, true, false, false); err != nil {
			t.Fatalf("write needle %d: %v", op.id, err)
		}
		f.written[op.id] = n
	}
	f.end, _, _ = v.DataBackend.GetStat()
	f.datName, f.idxName = v.FileName(".dat"), v.FileName(".idx")
	v.Close()
	if unindexed > 0 {
		if err := os.Truncate(f.idxName, fileSize(t, f.idxName)-int64(unindexed)*types.NeedleMapEntrySize); err != nil {
			t.Fatalf("drop %d .idx entries: %v", unindexed, err)
		}
	}
	return f
}

func (f *tornFixture) truncateDat(t *testing.T, size int64) {
	t.Helper()
	if err := os.Truncate(f.datName, size); err != nil {
		t.Fatal(err)
	}
}

// flipDataByte damages the data of the record at the given offset, so its
// CRC no longer matches.
func (f *tornFixture) flipDataByte(t *testing.T, at int64) {
	t.Helper()
	file, err := os.OpenFile(f.datName, os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	b := make([]byte, 1)
	pos := at + types.NeedleHeaderSize + 4 // past the 4-byte DataSize field
	if _, err := file.ReadAt(b, pos); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if _, err := file.WriteAt(b, pos); err != nil {
		t.Fatal(err)
	}
}

func assertReadable(t *testing.T, v *Volume, want *needle.Needle) {
	t.Helper()
	got := &needle.Needle{Id: want.Id}
	if _, err := v.readNeedle(got, nil, nil); err != nil {
		t.Fatalf("read needle %d: %v", want.Id, err)
	}
	if !bytes.Equal(got.Data, want.Data) {
		t.Fatalf("needle %d reads back different data", want.Id)
	}
}

func assertNotReadable(t *testing.T, v *Volume, id uint64) {
	t.Helper()
	if _, err := v.readNeedle(&needle.Needle{Id: types.Uint64ToNeedleId(id)}, nil, nil); err == nil {
		t.Fatalf("needle %d reads back; it should be deleted or absent", id)
	}
}

func writes(ids ...uint64) []tornOp {
	ops := make([]tornOp, len(ids))
	for i, id := range ids {
		ops[i] = tornOp{id: id}
	}
	return ops
}

func TestVolumeLoadRepairsUnindexedDatTail(t *testing.T) {
	cases := []struct {
		name      string
		ops       []tornOp
		unindexed int
		damage    func(t *testing.T, f *tornFixture)
		wantSize  func(f *tornFixture) int64 // .dat size after the repair
		reindexed int
		readable  []uint64
		absent    []uint64
	}{
		{name: "one complete unindexed needle is re-indexed",
			ops: writes(1, 2, 3, 4), unindexed: 1,
			wantSize: func(f *tornFixture) int64 { return f.end }, reindexed: 1,
			readable: []uint64{1, 2, 3, 4}},
		{name: "two complete unindexed needles are re-indexed",
			ops: writes(1, 2, 3, 4, 5), unindexed: 2,
			wantSize: func(f *tornFixture) int64 { return f.end }, reindexed: 2,
			readable: []uint64{1, 2, 3, 4, 5}},
		{name: "an unindexed delete is re-indexed as a tombstone",
			ops: append(writes(1, 2, 3), tornOp{id: 2, del: true}), unindexed: 1,
			wantSize: func(f *tornFixture) int64 { return f.end }, reindexed: 1,
			readable: []uint64{1, 3}, absent: []uint64{2}},
		{name: "a partial header is cut",
			ops: writes(1, 2, 3, 4), unindexed: 1,
			damage:   func(t *testing.T, f *tornFixture) { f.truncateDat(t, f.starts[3]+7) },
			wantSize: func(f *tornFixture) int64 { return f.starts[3] },
			readable: []uint64{1, 2, 3}, absent: []uint64{4}},
		{name: "a partial body is cut",
			ops: writes(1, 2, 3, 4), unindexed: 1,
			damage:   func(t *testing.T, f *tornFixture) { f.truncateDat(t, f.starts[3]+40) },
			wantSize: func(f *tornFixture) int64 { return f.starts[3] },
			readable: []uint64{1, 2, 3}, absent: []uint64{4}},
		{name: "a complete needle is re-indexed and the partial one after it cut",
			ops: writes(1, 2, 3, 4, 5), unindexed: 2,
			damage:   func(t *testing.T, f *tornFixture) { f.truncateDat(t, f.starts[4]+40) },
			wantSize: func(f *tornFixture) int64 { return f.starts[4] }, reindexed: 1,
			readable: []uint64{1, 2, 3, 4}, absent: []uint64{5}},
		{name: "a last needle with a bad CRC is cut",
			ops: writes(1, 2, 3, 4), unindexed: 1,
			damage:   func(t *testing.T, f *tornFixture) { f.flipDataByte(t, f.starts[3]) },
			wantSize: func(f *tornFixture) int64 { return f.starts[3] },
			readable: []uint64{1, 2, 3}, absent: []uint64{4}},
		{name: "a partial needle after an indexed tombstone is cut",
			ops: append(append(writes(1, 2, 3), tornOp{id: 2, del: true}), tornOp{id: 4}), unindexed: 1,
			damage:   func(t *testing.T, f *tornFixture) { f.truncateDat(t, f.starts[4]+40) },
			wantSize: func(f *tornFixture) int64 { return f.starts[4] },
			readable: []uint64{1, 3}, absent: []uint64{2, 4}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := buildTornFixture(t, tc.ops, tc.unindexed)
			if tc.damage != nil {
				tc.damage(t, f)
			}
			idxBefore := fileSize(t, f.idxName)

			v := newTornTailTestVolume(t, f.dir)
			defer v.Close()
			if v.noWriteOrDelete || v.noWriteCanDelete {
				t.Fatal("volume with an unindexed .dat tail loaded read-only")
			}
			if got, want := fileSize(t, f.datName), tc.wantSize(f); got != want {
				t.Fatalf(".dat is %d bytes after load, want %d", got, want)
			}
			if got, want := fileSize(t, f.idxName), idxBefore+int64(tc.reindexed)*types.NeedleMapEntrySize; got != want {
				t.Fatalf(".idx is %d bytes after load, want %d (%d entries re-indexed)", got, want, tc.reindexed)
			}
			for _, id := range tc.readable {
				assertReadable(t, v, f.written[id])
			}
			for _, id := range tc.absent {
				assertNotReadable(t, v, id)
			}
			extra := newRandomNeedle(100)
			if _, _, _, err := v.writeNeedle2(extra, true, false, false); err != nil {
				t.Fatalf("write after repair: %v", err)
			}
			assertReadable(t, v, extra)
			v.Close()

			// the repaired volume reloads clean, and keeps what it indexed
			v2 := newTornTailTestVolume(t, f.dir)
			defer v2.Close()
			if v2.noWriteOrDelete {
				t.Fatal("repaired volume loaded read-only on the next load")
			}
			for _, id := range tc.readable {
				assertReadable(t, v2, f.written[id])
			}
			assertReadable(t, v2, extra)
		})
	}
}

// Every tail an interrupted append does not explain is left exactly as it is,
// and the volume loads read-only as before.
func TestVolumeLoadKeepsUnexplainedTailReadOnly(t *testing.T) {
	cases := []struct {
		name      string
		ops       []tornOp
		unindexed int
		damage    func(t *testing.T, f *tornFixture)
	}{
		{name: "the last indexed needle's id differs from its entry",
			ops: writes(1, 2, 3, 4), unindexed: 1,
			damage: func(t *testing.T, f *tornFixture) {
				file, err := os.OpenFile(f.datName, os.O_RDWR, 0644)
				if err != nil {
					t.Fatal(err)
				}
				defer file.Close()
				if _, err := file.WriteAt([]byte{0xff}, f.starts[2]+types.CookieSize+types.NeedleIdSize-1); err != nil {
					t.Fatal(err)
				}
			}},
		{name: "the last indexed needle fails its CRC",
			ops: writes(1, 2, 3, 4), unindexed: 1,
			damage: func(t *testing.T, f *tornFixture) { f.flipDataByte(t, f.starts[2]) }},
		{name: "a bad CRC followed by more data",
			ops: writes(1, 2, 3, 4, 5), unindexed: 2,
			damage: func(t *testing.T, f *tornFixture) { f.flipDataByte(t, f.starts[3]) }},
		{name: "zero bytes past the last needle (id 0)",
			ops: writes(1, 2, 3),
			damage: func(t *testing.T, f *tornFixture) {
				file, err := os.OpenFile(f.datName, os.O_RDWR|os.O_APPEND, 0644)
				if err != nil {
					t.Fatal(err)
				}
				defer file.Close()
				if _, err := file.Write(make([]byte, 64)); err != nil {
					t.Fatal(err)
				}
			}},
		{name: "the index ends past the data",
			ops:    writes(1, 2, 3),
			damage: func(t *testing.T, f *tornFixture) { f.truncateDat(t, f.end-10) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := buildTornFixture(t, tc.ops, tc.unindexed)
			tc.damage(t, f)
			datBefore, idxBefore := fileSize(t, f.datName), fileSize(t, f.idxName)

			v := newTornTailTestVolume(t, f.dir)
			defer v.Close()
			if !v.noWriteOrDelete {
				t.Fatal("volume with an unexplained tail loaded writable")
			}
			if got := fileSize(t, f.datName); got != datBefore {
				t.Fatalf(".dat changed from %d to %d bytes", datBefore, got)
			}
			if got := fileSize(t, f.idxName); got != idxBefore {
				t.Fatalf(".idx changed from %d to %d bytes", idxBefore, got)
			}
		})
	}
}

func TestVolumeLoadLeavesCleanVolumeUntouched(t *testing.T) {
	f := buildTornFixture(t, append(writes(1, 2, 3), tornOp{id: 2, del: true}), 0)
	datBefore, idxBefore := fileSize(t, f.datName), fileSize(t, f.idxName)

	v := newTornTailTestVolume(t, f.dir)
	defer v.Close()
	if v.noWriteOrDelete || v.noWriteCanDelete {
		t.Fatal("clean volume loaded read-only")
	}
	if got := fileSize(t, f.datName); got != datBefore {
		t.Fatalf("clean .dat changed from %d to %d bytes", datBefore, got)
	}
	if got := fileSize(t, f.idxName); got != idxBefore {
		t.Fatalf("clean .idx changed from %d to %d bytes", idxBefore, got)
	}
	assertReadable(t, v, f.written[1])
	assertReadable(t, v, f.written[3])
	assertNotReadable(t, v, 2)
}

// A volume persisted read-only is not repaired: the tail stays for whoever
// marked it.
func TestVolumeLoadLeavesReadOnlyVolumeTailAlone(t *testing.T) {
	dir := t.TempDir()
	v := newTornTailTestVolume(t, dir)
	for _, id := range []uint64{1, 2, 99} {
		if _, _, _, err := v.writeNeedle2(newRandomNeedle(id), true, false, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := v.PersistReadOnly(true, false); err != nil {
		t.Fatal(err)
	}
	datName, idxName := v.FileName(".dat"), v.FileName(".idx")
	v.Close()
	if err := os.Truncate(idxName, fileSize(t, idxName)-types.NeedleMapEntrySize); err != nil {
		t.Fatal(err)
	}
	datBefore, idxBefore := fileSize(t, datName), fileSize(t, idxName)

	v = newTornTailTestVolume(t, dir)
	defer v.Close()
	if !v.noWriteOrDelete {
		t.Fatal("volume persisted read-only loaded writable")
	}
	if got := fileSize(t, datName); got != datBefore {
		t.Fatalf(".dat of a read-only volume changed from %d to %d bytes", datBefore, got)
	}
	if got := fileSize(t, idxName); got != idxBefore {
		t.Fatalf(".idx of a read-only volume changed from %d to %d bytes", idxBefore, got)
	}
}

// An .idx that ends in a partial entry keeps the existing behaviour:
// the volume does not load writable and neither file is changed.
func TestVolumeLoadLeavesPartialIdxEntryAlone(t *testing.T) {
	f := buildTornFixture(t, writes(1, 2, 3), 0)
	idxPartial := fileSize(t, f.idxName) - types.NeedleMapEntrySize/2
	if err := os.Truncate(f.idxName, idxPartial); err != nil {
		t.Fatal(err)
	}
	datBefore := fileSize(t, f.datName)

	v2, err := NewVolume(f.dir, f.dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err == nil {
		t.Logf("partial .idx: volume loaded, noWriteOrDelete=%v", v2.noWriteOrDelete)
		if !v2.noWriteOrDelete {
			v2.Close()
			t.Fatal("volume with a partial .idx entry loaded writable")
		}
		v2.Close()
	} else {
		t.Logf("partial .idx: volume not loaded: %v", err)
	}
	if got := fileSize(t, f.datName); got != datBefore {
		t.Fatalf(".dat changed from %d to %d bytes", datBefore, got)
	}
	if got := fileSize(t, f.idxName); got != idxPartial {
		t.Fatalf(".idx changed from %d to %d bytes", idxPartial, got)
	}
}

// faultyDatFile fails Sync, or the read of a whole record at one offset, the
// way a failing disk does.
type faultyDatFile struct {
	backend.BackendStorageFile
	failSync      bool
	failRecordAt  int64
	failRecordSet bool
}

var errFaultyDisk = errors.New("input/output error")

func (f *faultyDatFile) Sync() error {
	if f.failSync {
		return errFaultyDisk
	}
	return f.BackendStorageFile.Sync()
}

func (f *faultyDatFile) ReadAt(p []byte, off int64) (int, error) {
	if f.failRecordSet && off == f.failRecordAt && len(p) > types.NeedleHeaderSize {
		return 0, errFaultyDisk
	}
	return f.BackendStorageFile.ReadAt(p, off)
}

// A read error is not evidence of a torn write: the last record is kept.
func TestWalkUnindexedDatTailKeepsRecordOnReadError(t *testing.T) {
	f := buildTornFixture(t, writes(1, 2, 3, 4), 1)
	file, err := os.Open(f.datName)
	if err != nil {
		t.Fatal(err)
	}
	dat := &faultyDatFile{BackendStorageFile: backend.NewDiskFile(file), failRecordAt: f.starts[3], failRecordSet: true}
	defer dat.Close()

	records, cutAt, err := walkUnindexedDatTail(dat, needle.Version3, f.starts[3], f.end, 0)
	if err == nil {
		t.Fatalf("read error on the last record: walk returned %d record(s) and a cut at %d, want an error", len(records), cutAt)
	}
	if errors.Is(err, errTailNotTorn) {
		t.Fatalf("read error reported as an unexplained tail: %v", err)
	}
	if !errors.Is(err, errFaultyDisk) {
		t.Fatalf("walk error %v does not carry the read error", err)
	}
}

// A repair that changed a file but could not sync it fails the integrity
// check, so the volume loads read-only rather than writable on an unsynced
// repair.
func TestCheckVolumeDataIntegrityFailsOnUnsyncedRepair(t *testing.T) {
	dir := t.TempDir()
	v := newTornTailTestVolume(t, dir)
	var starts []int64
	for _, id := range []uint64{1, 2, 3, 4} {
		at, _, _ := v.DataBackend.GetStat()
		starts = append(starts, at)
		if _, _, _, err := v.writeNeedle2(newRandomNeedle(id), true, false, false); err != nil {
			t.Fatal(err)
		}
	}
	// persisted read-only so the load below does not repair the tail itself
	if err := v.PersistReadOnly(true, false); err != nil {
		t.Fatal(err)
	}
	datName, idxName := v.FileName(".dat"), v.FileName(".idx")
	v.Close()
	if err := os.Truncate(idxName, fileSize(t, idxName)-types.NeedleMapEntrySize); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(datName, starts[3]+40); err != nil {
		t.Fatal(err)
	}

	v = newTornTailTestVolume(t, dir)
	defer v.Close()
	v.noWriteOrDelete = false
	v.DataBackend = &faultyDatFile{BackendStorageFile: v.DataBackend, failSync: true}
	indexFile, err := os.OpenFile(idxName, os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer indexFile.Close()

	if _, err := CheckVolumeDataIntegrity(v, indexFile); err == nil {
		t.Fatal("integrity check passed although the repaired .dat was not synced")
	}
}

// partialWriteIndex accepts only the first n bytes of a write, as a full disk
// does.
type partialWriteIndex struct {
	data      []byte
	accept    int
	truncated bool
}

func (p *partialWriteIndex) WriteAt(b []byte, off int64) (int, error) {
	n := min(len(b), p.accept)
	if end := int(off) + n; end > len(p.data) {
		p.data = append(p.data, make([]byte, end-len(p.data))...)
	}
	copy(p.data[off:], b[:n])
	if n < len(b) {
		return n, errors.New("no space left on device")
	}
	return n, nil
}

func (p *partialWriteIndex) Truncate(size int64) error {
	p.data = p.data[:size]
	p.truncated = true
	return nil
}

func (p *partialWriteIndex) Sync() error  { return nil }
func (p *partialWriteIndex) Name() string { return "test.idx" }

// A failed append leaves no partial entry behind.
func TestAppendIndexEntriesRemovesPartialWrite(t *testing.T) {
	idxFile := &partialWriteIndex{data: make([]byte, 2*types.NeedleMapEntrySize), accept: types.NeedleMapEntrySize + 5}
	entries := make([]byte, 2*types.NeedleMapEntrySize)
	if err := appendIndexEntries(idxFile, entries, 2*types.NeedleMapEntrySize); err == nil {
		t.Fatal("a partial append reported success")
	}
	if got, want := len(idxFile.data), 2*types.NeedleMapEntrySize; got != want {
		t.Fatalf(".idx is %d bytes after a failed append, want %d", got, want)
	}
}
