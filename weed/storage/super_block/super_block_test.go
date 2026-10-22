package super_block

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func TestSuperBlockReadWrite(t *testing.T) {
	rp, _ := NewReplicaPlacementFromByte(byte(001))
	ttl, _ := needle.ReadTTL("15d")
	s := &SuperBlock{
		Version:          needle.GetCurrentVersion(),
		ReplicaPlacement: rp,
		Ttl:              ttl,
	}

	bytes := s.Bytes()

	if !(bytes[2] == 15 && bytes[3] == needle.Day) {
		println("byte[2]:", bytes[2], "byte[3]:", bytes[3])
		t.Fail()
	}

}

func TestReadSuperBlockWithExtra(t *testing.T) {
	rp, _ := NewReplicaPlacementFromByte(byte(001))
	ttl, _ := needle.ReadTTL("15d")
	s := &SuperBlock{
		Version:          needle.GetCurrentVersion(),
		ReplicaPlacement: rp,
		Ttl:              ttl,
		Extra: &master_pb.SuperBlockExtra{
			ErasureCoding: &master_pb.SuperBlockExtra_ErasureCoding{
				Data:      10,
				Parity:    4,
				VolumeIds: []uint32{1, 2, 3},
			},
		},
	}

	datPath := filepath.Join(t.TempDir(), "1.dat")
	if err := os.WriteFile(datPath, s.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(datPath)
	if err != nil {
		t.Fatal(err)
	}
	datBackend := backend.NewDiskFile(f)
	defer datBackend.Close()

	got, err := ReadSuperBlock(datBackend)
	if err != nil {
		t.Fatalf("ReadSuperBlock: %v", err)
	}
	if got.ExtraSize != s.ExtraSize {
		t.Errorf("ExtraSize = %d, want %d", got.ExtraSize, s.ExtraSize)
	}
	if !proto.Equal(got.Extra, s.Extra) {
		t.Errorf("Extra = %v, want %v", got.Extra, s.Extra)
	}
}

// io.ReaderAt may return io.EOF on a full read; a remote backend can pass that through.
type eofBackendStorageFile struct {
	backend.DiskFile
	data []byte
}

func (f *eofBackendStorageFile) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= int64(len(f.data)) {
		return 0, io.EOF
	}
	return copy(p, f.data[off:]), io.EOF
}

func TestReadSuperBlockExtraFullReadWithEOF(t *testing.T) {
	rp, _ := NewReplicaPlacementFromByte(byte(001))
	ttl, _ := needle.ReadTTL("15d")
	s := &SuperBlock{
		Version:          needle.GetCurrentVersion(),
		ReplicaPlacement: rp,
		Ttl:              ttl,
		Extra: &master_pb.SuperBlockExtra{
			ErasureCoding: &master_pb.SuperBlockExtra_ErasureCoding{
				Data:      10,
				Parity:    4,
				VolumeIds: []uint32{1, 2, 3},
			},
		},
	}

	got, err := ReadSuperBlock(&eofBackendStorageFile{data: s.Bytes()})
	if err != nil {
		t.Fatalf("ReadSuperBlock: %v", err)
	}
	if !proto.Equal(got.Extra, s.Extra) {
		t.Errorf("Extra = %v, want %v", got.Extra, s.Extra)
	}
}

func TestReadSuperBlockTruncatedExtra(t *testing.T) {
	rp, _ := NewReplicaPlacementFromByte(byte(001))
	ttl, _ := needle.ReadTTL("15d")
	s := &SuperBlock{
		Version:          needle.GetCurrentVersion(),
		ReplicaPlacement: rp,
		Ttl:              ttl,
		Extra: &master_pb.SuperBlockExtra{
			ErasureCoding: &master_pb.SuperBlockExtra_ErasureCoding{
				Data:      10,
				Parity:    4,
				VolumeIds: []uint32{1, 2, 3},
			},
		},
	}

	data := s.Bytes()
	datPath := filepath.Join(t.TempDir(), "1.dat")
	if err := os.WriteFile(datPath, data[:len(data)-1], 0644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(datPath)
	if err != nil {
		t.Fatal(err)
	}
	datBackend := backend.NewDiskFile(f)
	defer datBackend.Close()

	_, err = ReadSuperBlock(datBackend)
	if err == nil || !strings.Contains(err.Error(), "cannot read") {
		t.Fatalf("expected read error for truncated extra, got %v", err)
	}
}

// s3_backend.ReadAt clears io.EOF even when it returns fewer bytes than requested.
type shortReadBackendStorageFile struct {
	backend.DiskFile
	data []byte
}

func (f *shortReadBackendStorageFile) ReadAt(p []byte, off int64) (n int, err error) {
	return copy(p, f.data[off:]), nil
}

func TestReadSuperBlockShortReadWithoutError(t *testing.T) {
	rp, _ := NewReplicaPlacementFromByte(byte(001))
	ttl, _ := needle.ReadTTL("15d")
	s := &SuperBlock{
		Version:          needle.GetCurrentVersion(),
		ReplicaPlacement: rp,
		Ttl:              ttl,
		Extra: &master_pb.SuperBlockExtra{
			ErasureCoding: &master_pb.SuperBlockExtra_ErasureCoding{
				Data:      10,
				Parity:    4,
				VolumeIds: []uint32{1, 2, 3},
			},
		},
	}

	data := s.Bytes()
	_, err := ReadSuperBlock(&shortReadBackendStorageFile{data: data[:len(data)-1]})
	if err == nil || !strings.Contains(err.Error(), "cannot read") {
		t.Fatalf("expected read error for short extra read, got %v", err)
	}
}
