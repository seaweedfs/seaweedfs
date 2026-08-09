package storage

import (
	"runtime"
	"sync"
	"testing"
	"unsafe"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func stringData(s string) uintptr {
	if s == "" {
		return 0
	}
	return uintptr(unsafe.Pointer(unsafe.StringData(s)))
}

// Every heartbeat decodes a fresh string for values a cluster repeats across
// all its volumes, so a master holding a million volumes would otherwise hold a
// million copies of the same handful of names.
func TestRepeatedVolumeStringsAreShared(t *testing.T) {
	message := func() *master_pb.VolumeInformationMessage {
		return &master_pb.VolumeInformationMessage{
			Id: 1, Version: 3,
			// Built from bytes so each is a distinct allocation, as decoding is.
			Collection:        string([]byte("somecollection")),
			DiskType:          string([]byte("ssd")),
			RemoteStorageName: string([]byte("s3cold")),
			RemoteStorageKey:  string([]byte("seaweed/somecollection/1.dat")),
		}
	}

	first, err := NewVolumeInfo(message())
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewVolumeInfo(message())
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name  string
		a, b  string
		share bool
	}{
		{"Collection", first.Collection, second.Collection, true},
		{"DiskType", first.DiskType, second.DiskType, true},
		{"RemoteStorageName", first.RemoteStorageName, second.RemoteStorageName, true},
		// Unique per volume: interning it would fill the table rather than
		// share anything.
		{"RemoteStorageKey", first.RemoteStorageKey, second.RemoteStorageKey, false},
	} {
		if tc.a != tc.b {
			t.Fatalf("%s: values differ, %q vs %q", tc.name, tc.a, tc.b)
		}
		if shared := stringData(tc.a) == stringData(tc.b); shared != tc.share {
			t.Errorf("%s: shared=%v, want %v", tc.name, shared, tc.share)
		}
	}
}

func TestShortVolumeInfoSharesTheSameStrings(t *testing.T) {
	message := func() *master_pb.VolumeShortInformationMessage {
		return &master_pb.VolumeShortInformationMessage{
			Id: 1, Version: 3,
			Collection: string([]byte("somecollection")),
			DiskType:   string([]byte("ssd")),
		}
	}
	first, err := NewVolumeInfoFromShort(message())
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewVolumeInfoFromShort(message())
	if err != nil {
		t.Fatal(err)
	}
	if stringData(first.Collection) != stringData(second.Collection) {
		t.Error("collection is not shared between volumes reported as a delta")
	}
	if stringData(first.DiskType) != stringData(second.DiskType) {
		t.Error("disk type is not shared between volumes reported as a delta")
	}
}

func TestEmptyVolumeStringsStayEmpty(t *testing.T) {
	vi, err := NewVolumeInfo(&master_pb.VolumeInformationMessage{Id: 1, Version: 3})
	if err != nil {
		t.Fatal(err)
	}
	if vi.Collection != "" || vi.DiskType != "" || vi.RemoteStorageName != "" {
		t.Errorf("expected empty strings to survive, got %+v", vi)
	}
	if vi.IsRemote() {
		t.Error("a volume with no remote backend reads as remote")
	}
}

// Sharing has to survive collection: volumes are interned when they are first
// reported, and in a cluster sending only what changed most are never reported
// again. A table that let its entries be collected would hand the next volume
// a second copy of a name the rest of the cluster already shares.
func TestVolumeStringsStaySharedAcrossCollection(t *testing.T) {
	first, err := NewVolumeInfo(&master_pb.VolumeInformationMessage{
		Id: 1, Version: 3, Collection: string([]byte("somecollection")),
	})
	if err != nil {
		t.Fatal(err)
	}
	original := stringData(first.Collection)

	for i := 0; i < 5; i++ {
		runtime.GC()
	}

	later, err := NewVolumeInfo(&master_pb.VolumeInformationMessage{
		Id: 2, Version: 3, Collection: string([]byte("somecollection")),
	})
	if err != nil {
		t.Fatal(err)
	}
	if stringData(later.Collection) != original {
		t.Error("a volume reported after a collection got its own copy of the name")
	}
	runtime.KeepAlive(first)
}

func TestInterningIsSafeUnderConcurrentReports(t *testing.T) {
	var wg sync.WaitGroup
	shared := make([]uintptr, 16)
	for i := range shared {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			vi, err := NewVolumeInfo(&master_pb.VolumeInformationMessage{
				Id: uint32(i), Version: 3, Collection: string([]byte("concurrent")),
			})
			if err != nil {
				t.Error(err)
				return
			}
			shared[i] = stringData(vi.Collection)
		}(i)
	}
	wg.Wait()
	for i, got := range shared {
		if got != shared[0] {
			t.Fatalf("report %d got its own copy of the name", i)
		}
	}
}
