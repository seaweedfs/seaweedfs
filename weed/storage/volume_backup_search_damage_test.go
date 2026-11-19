package storage

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// searchDamageLayout writes keys 1..30, deletes ten of them, then writes keys
// 31..34: 44 index rows. BinarySearchByAppendAtNs probes row 22 first on
// every search, and row 22 is the write row of key 23, so a damaged body of
// key 23 lies on the path of every search.
const (
	searchDamageFirstKeys = 30
	searchDamageLaterKeys = 4
	searchDamageProbeRow  = 22
	searchDamageProbeKey  = 23
)

// buildSearchDamageVolume builds the layout above in dir, deleting keys
// deleteFrom..deleteFrom+9, and closes the volume. It returns the data each
// live key holds.
func buildSearchDamageVolume(t *testing.T, dir string, version needle.Version, deleteFrom uint64) map[uint64][]byte {
	t.Helper()
	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, version, 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()

	rnd := rand.New(rand.NewSource(7))
	live := map[uint64][]byte{}
	write := func(k uint64) {
		n := new(needle.Needle)
		n.Id = types.Uint64ToNeedleId(k)
		n.Cookie = types.Cookie(rnd.Uint32())
		n.Data = make([]byte, 256+rnd.Intn(512))
		rnd.Read(n.Data)
		n.Checksum = needle.NewCRC(n.Data)
		if _, _, _, err := v.writeNeedle2(n, true, false, false); err != nil {
			t.Fatalf("write needle %d: %v", k, err)
		}
		live[k] = n.Data
	}
	for k := uint64(1); k <= searchDamageFirstKeys; k++ {
		write(k)
	}
	for k := deleteFrom; k < deleteFrom+10; k++ {
		if _, err := v.deleteNeedle2(newEmptyNeedle(k), false); err != nil {
			t.Fatalf("delete needle %d: %v", k, err)
		}
		delete(live, k)
	}
	for k := uint64(searchDamageFirstKeys + 1); k <= searchDamageFirstKeys+searchDamageLaterKeys; k++ {
		write(k)
	}
	return live
}

func copyVolumeFiles(t *testing.T, from, to string) {
	t.Helper()
	for _, ext := range []string{".dat", ".idx", ".vif"} {
		data, err := os.ReadFile(VolumeFileName(from, "", 1) + ext)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(to, filepath.Base(VolumeFileName(from, "", 1)+ext)), data, 0644); err != nil {
			t.Fatal(err)
		}
	}
}

// damageNeedleData flips one byte in the middle of the data of the record the
// write row searchDamageProbeRow names. Header and trailer stay intact. It
// returns the record's actual offset and size.
func damageNeedleData(t *testing.T, dir string) (int64, types.Size) {
	t.Helper()
	base := VolumeFileName(dir, "", 1)
	idxBytes, err := os.ReadFile(base + ".idx")
	if err != nil {
		t.Fatal(err)
	}
	row := idxBytes[searchDamageProbeRow*types.NeedleMapEntrySize : (searchDamageProbeRow+1)*types.NeedleMapEntrySize]
	key, offset, size := idx.IdxFileEntry(row)
	if key != types.Uint64ToNeedleId(searchDamageProbeKey) || offset.IsZero() || size <= 0 || size.IsDeleted() {
		t.Fatalf("index row %d is key %d offset %d size %d, want the write row of key %d", searchDamageProbeRow, key, offset.ToActualOffset(), size, searchDamageProbeKey)
	}
	f, err := os.OpenFile(base+".dat", os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	// the data follows the header and the 4-byte DataSize field
	dataSizeBytes := make([]byte, 4)
	if _, err := f.ReadAt(dataSizeBytes, offset.ToActualOffset()+types.NeedleHeaderSize); err != nil {
		t.Fatal(err)
	}
	dataSize := int64(dataSizeBytes[0])<<24 | int64(dataSizeBytes[1])<<16 | int64(dataSizeBytes[2])<<8 | int64(dataSizeBytes[3])
	at := offset.ToActualOffset() + types.NeedleHeaderSize + 4 + dataSize/2
	b := make([]byte, 1)
	if _, err := f.ReadAt(b, at); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b, at); err != nil {
		t.Fatal(err)
	}
	return offset.ToActualOffset(), size
}

func loadVolume(t *testing.T, dir string) *Volume {
	t.Helper()
	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, nil, nil, 0, 0, 0, 0)
	if err != nil {
		t.Fatalf("load volume in %s: %v", dir, err)
	}
	t.Cleanup(v.Close)
	return v
}

// searchTargets returns 0, the largest timestamp, and the append time of
// every record the index names, read from the undamaged volume.
func searchTargets(t *testing.T, ref *Volume) []uint64 {
	t.Helper()
	targets := []uint64{0, math.MaxUint64}
	rows := int64(ref.IndexFileSize()) / types.NeedleMapEntrySize
	for i := int64(0); i < rows; i++ {
		offset, err := ref.readOffsetFromIndex(i)
		if err != nil {
			t.Fatal(err)
		}
		if offset.IsZero() {
			continue
		}
		ts, err := ref.readAppendAtNs(offset)
		if err != nil {
			t.Fatalf("reference row %d: %v", i, err)
		}
		targets = append(targets, ts-1, ts)
	}
	return targets
}

func assertSameSearchResults(t *testing.T, damaged, ref *Volume) {
	t.Helper()
	for _, ts := range searchTargets(t, ref) {
		wantOffset, wantLast, wantErr := ref.BinarySearchByAppendAtNs(ts)
		if wantErr != nil {
			t.Fatalf("search %d on the undamaged volume: %v", ts, wantErr)
		}
		gotOffset, gotLast, gotErr := damaged.BinarySearchByAppendAtNs(ts)
		if gotErr != nil {
			t.Fatalf("search %d on the damaged volume: %v", ts, gotErr)
		}
		if gotOffset != wantOffset || gotLast != wantLast {
			t.Fatalf("search %d: damaged volume gives offset %d last %v, undamaged gives offset %d last %v",
				ts, gotOffset.ToActualOffset(), gotLast, wantOffset.ToActualOffset(), wantLast)
		}
	}
}

func assertBodyDamaged(t *testing.T, v *Volume, actualOffset int64, size types.Size) {
	t.Helper()
	n := new(needle.Needle)
	err := n.ReadData(v.DataBackend, actualOffset, size, v.Version())
	if err == nil || !strings.Contains(err.Error(), "CRC") {
		t.Fatalf("inconclusive: the damaged record at %d reads with err %v, want a CRC error", actualOffset, err)
	}
}

// A damaged body of a deleted needle does not change what the append-time
// search finds.
func TestBinarySearchByAppendAtNsDamagedDeletedBody(t *testing.T) {
	for _, version := range []needle.Version{needle.Version3} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			damagedDir, refDir := t.TempDir(), t.TempDir()
			live := buildSearchDamageVolume(t, damagedDir, version, 16)
			if _, ok := live[searchDamageProbeKey]; ok {
				t.Fatalf("layout: key %d must be deleted", searchDamageProbeKey)
			}
			copyVolumeFiles(t, damagedDir, refDir)
			actualOffset, size := damageNeedleData(t, damagedDir)

			damaged, ref := loadVolume(t, damagedDir), loadVolume(t, refDir)
			assertBodyDamaged(t, damaged, actualOffset, size)
			assertSameSearchResults(t, damaged, ref)
		})
	}
}

// A damaged body of a live needle does not change what the append-time
// search finds, and a normal read of that needle still fails its CRC.
func TestBinarySearchByAppendAtNsDamagedLiveBody(t *testing.T) {
	for _, version := range []needle.Version{needle.Version3} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			damagedDir, refDir := t.TempDir(), t.TempDir()
			live := buildSearchDamageVolume(t, damagedDir, version, 1)
			if _, ok := live[searchDamageProbeKey]; !ok {
				t.Fatalf("layout: key %d must be live", searchDamageProbeKey)
			}
			copyVolumeFiles(t, damagedDir, refDir)
			actualOffset, size := damageNeedleData(t, damagedDir)

			damaged, ref := loadVolume(t, damagedDir), loadVolume(t, refDir)
			assertBodyDamaged(t, damaged, actualOffset, size)
			assertSameSearchResults(t, damaged, ref)

			n := newEmptyNeedle(searchDamageProbeKey)
			if _, err := damaged.readNeedle(n, nil, nil); err == nil || !strings.Contains(err.Error(), "CRC") {
				t.Fatalf("read of the damaged live needle %d: err %v, want a CRC error", searchDamageProbeKey, err)
			}
			for k, data := range live {
				if k == searchDamageProbeKey {
					continue
				}
				n := newEmptyNeedle(k)
				if _, err := damaged.readNeedle(n, nil, nil); err != nil || string(n.Data) != string(data) {
					t.Fatalf("read of undamaged live needle %d: err %v, data equal %v", k, err, string(n.Data) == string(data))
				}
			}
		})
	}
}
