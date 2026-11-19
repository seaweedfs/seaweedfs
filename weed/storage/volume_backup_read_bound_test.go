package storage

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// readRecorder records the byte range of every ReadAt on a data backend.
type readRecorder struct {
	backend.BackendStorageFile
	reads [][2]int64 // start, end
}

func (r *readRecorder) ReadAt(p []byte, off int64) (int, error) {
	r.reads = append(r.reads, [2]int64{off, off + int64(len(p))})
	return r.BackendStorageFile.ReadAt(p, off)
}

// readNeedleAt returns the id, cookie, size, checksum and append time of a
// record and reads only its header and its trailer: no read touches the
// bytes between them. An implementation that reads the whole record fails
// the byte-range assertions, whether or not it verifies the data checksum.
func TestReadNeedleAtReadsHeaderAndTrailerOnly(t *testing.T) {
	for _, version := range []needle.Version{needle.Version3} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			dir := t.TempDir()
			live := buildSearchDamageVolume(t, dir, version, 16)
			v := loadVolume(t, dir)

			checked := 0
			for key := range live {
				nv, ok := v.nm.Get(types.Uint64ToNeedleId(key))
				if !ok || nv.Size <= 0 {
					t.Fatalf("key %d is not live in the needle map", key)
				}
				actualOffset := nv.Offset.ToActualOffset()

				// What the record holds, from a full parse on the plain backend.
				want, _, bodyLength, err := needle.ReadNeedleHeader(v.DataBackend, version, actualOffset)
				if err != nil {
					t.Fatalf("key %d: reference header: %v", key, err)
				}
				if _, err = want.ReadNeedleBody(v.DataBackend, version, actualOffset+types.NeedleHeaderSize, bodyLength); err != nil {
					t.Fatalf("key %d: reference body: %v", key, err)
				}
				if want.AppendAtNs == 0 {
					t.Fatalf("key %d: reference append time is zero", key)
				}

				plain := v.DataBackend
				rec := &readRecorder{BackendStorageFile: plain}
				v.DataBackend = rec
				got, err := v.readNeedleAt(nv.Offset)
				v.DataBackend = plain
				if err != nil {
					t.Fatalf("key %d: readNeedleAt: %v", key, err)
				}

				if got.Id != want.Id || got.Cookie != want.Cookie || got.Size != want.Size ||
					got.AppendAtNs != want.AppendAtNs || got.Checksum != want.Checksum {
					t.Errorf("key %d: readNeedleAt returned id %v cookie %x size %d appendAtNs %d checksum %x; the record holds id %v cookie %x size %d appendAtNs %d checksum %x",
						key, got.Id, got.Cookie, got.Size, got.AppendAtNs, got.Checksum,
						want.Id, want.Cookie, want.Size, want.AppendAtNs, want.Checksum)
				}
				if len(got.Data) != 0 {
					t.Errorf("key %d: readNeedleAt returned %d data bytes; want none", key, len(got.Data))
				}

				bodyStart := actualOffset + types.NeedleHeaderSize
				bodyEnd := bodyStart + int64(want.Size)
				trailerLength := bodyLength - int64(want.Size)
				var total int64
				for _, r := range rec.reads {
					total += r[1] - r[0]
					if r[0] < bodyEnd && r[1] > bodyStart {
						t.Errorf("key %d: read [%d,%d) touches the record's body [%d,%d)", key, r[0], r[1], bodyStart, bodyEnd)
					}
					if r[0] < actualOffset || r[1] > bodyStart+bodyLength {
						t.Errorf("key %d: read [%d,%d) is outside the record [%d,%d)", key, r[0], r[1], actualOffset, bodyStart+bodyLength)
					}
				}
				if limit := int64(types.NeedleHeaderSize) + trailerLength; total > limit {
					t.Errorf("key %d: read %d bytes; header and trailer are %d bytes", key, total, limit)
				}
				if len(rec.reads) == 0 {
					t.Errorf("key %d: no read was recorded", key)
				}
				checked++
			}
			if checked == 0 {
				t.Fatal("no live needle was checked")
			}
		})
	}
}
