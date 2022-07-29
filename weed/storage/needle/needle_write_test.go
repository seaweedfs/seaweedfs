package needle

import (
	"os"
	"testing"

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

	offset, _, _, _ := n.Append(datBackend, CurrentVersion)
	if offset != uint64(fileSize) {
		t.Errorf("Fail to Append Needle.")
	}
}
