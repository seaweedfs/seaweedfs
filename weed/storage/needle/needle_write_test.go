package needle

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestAppend(t *testing.T) {
	n := &Needle{

		Cookie:       types.Cookie(123),   // Cookie Cookie   `comment:"random number to mitigate brute force lookups"`
		Id:           types.NeedleId(123), // Id     NeedleId `comment:"needle id"`
		Size:         8,                   // Size   uint32   `comment:"sum of DataSize,Data,NameSize,Name,MimeSize,Mime"`
		DataSize:     4,                   // DataSize     uint32 `comment:"Data size"` //version2
		Data:         []byte("abcd"),      // Data         []byte `comment:"The actual file data"`
		Flags:        0,                   // Flags        byte   `comment:"boolean flags"`          //version2
		NameSize:     0,                   // NameSize     uint8                                     //version2
		Name:         nil,                 // Name         []byte `comment:"maximum 256 characters"` //version2
		MimeSize:     0,                   // MimeSize     uint8                                     //version2
		Mime:         nil,                 // Mime         []byte `comment:"maximum 256 characters"` //version2
		PairsSize:    0,                   // PairsSize    uint16                                    //version2
		Pairs:        nil,                 // Pairs        []byte `comment:"additional name value pairs, json format, maximum 6
		LastModified: 123,                 // LastModified uint64 //only store LastModifiedBytesLength bytes, which is 5 bytes
		Ttl:          nil,                 // Ttl          *TTL
		Checksum:     123,                 // Checksum   CRC    `comment:"CRC32 to check integrity"`
		AppendAtNs:   123,                 // AppendAtNs uint64 `comment:"append timestamp in nano seconds"` //version3
		Padding:      nil,                 // Padding    []byte `comment:"Aligned to 8 bytes"`
	}

	tempFile, err := os.CreateTemp("", ".dat")
	if err != nil {
		t.Errorf("Fail TempFile. %v", err)
		return
	}

	/*
		uint8  : 0 to 255
		uint16 : 0 to 65535
		uint32 : 0 to 4294967295
		uint64 : 0 to 18446744073709551615
		int8   : -128 to 127
		int16  : -32768 to 32767
		int32  : -2147483648 to 2147483647
		int64  : -9223372036854775808 to 9223372036854775807
	*/

	fileSize := int64(4294967296) + 10000
	tempFile.Truncate(fileSize)
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	datBackend := backend.NewDiskFile(tempFile)
	defer datBackend.Close()

	offset, _, _, _ := n.Append(datBackend, GetCurrentVersion())
	if offset != uint64(fileSize) {
		t.Errorf("Fail to Append Needle.")
	}
}

func versionString(v Version) string {
	switch v {
	case Version1:
		return "Version1"
	case Version2:
		return "Version2"
	case Version3:
		return "Version3"
	default:
		return "UnknownVersion"
	}
}

func TestWriteNeedle_CompatibilityWithLegacy(t *testing.T) {
	versions := []Version{Version1, Version2, Version3}
	for _, version := range versions {
		t.Run(versionString(version), func(t *testing.T) {
			n := &Needle{
				Cookie:       0x12345678,
				Id:           0x1122334455667788,
				Data:         []byte("hello world"),
				Flags:        0xFF,
				Name:         []byte("filename.txt"),
				Mime:         []byte("text/plain"),
				LastModified: 0x1234567890,
				Ttl:          nil, // Add TTL if needed
				Pairs:        []byte("key=value"),
				PairsSize:    9,
				Checksum:     0xCAFEBABE,
				AppendAtNs:   0xDEADBEEF,
			}

			// Legacy
			legacyBuf := &bytes.Buffer{}
			_, _, err := n.LegacyPrepareWriteBuffer(version, legacyBuf)
			if err != nil {
				t.Fatalf("LegacyPrepareWriteBuffer failed: %v", err)
			}

			// New
			newBuf := &bytes.Buffer{}
			offset := uint64(0)
			switch version {
			case Version1:
				_, _, err = writeNeedleV1(n, offset, newBuf)
			case Version2:
				_, _, err = writeNeedleV2(n, offset, newBuf)
			case Version3:
				_, _, err = writeNeedleV3(n, offset, newBuf)
			}
			if err != nil {
				t.Fatalf("writeNeedleV%d failed: %v", version, err)
			}

			if !bytes.Equal(legacyBuf.Bytes(), newBuf.Bytes()) {
				t.Errorf("Data layout mismatch for version %d\nLegacy: %x\nNew:    %x", version, legacyBuf.Bytes(), newBuf.Bytes())
			}
		})
	}
}

type mockBackendWriter struct {
	buf *bytes.Buffer
}

func (m *mockBackendWriter) WriteAt(p []byte, off int64) (n int, err error) {
	return m.buf.Write(p)
}

func (m *mockBackendWriter) GetStat() (int64, time.Time, error) {
	return 0, time.Time{}, nil
}

func (m *mockBackendWriter) Truncate(size int64) error {
	return nil
}

func (m *mockBackendWriter) Name() string {
	return "mock"
}

func (m *mockBackendWriter) Close() error {
	return nil
}

func (m *mockBackendWriter) Sync() error {
	return nil
}

func (m *mockBackendWriter) ReadAt(p []byte, off int64) (n int, err error) {
	// Not used in this test
	return 0, nil
}
