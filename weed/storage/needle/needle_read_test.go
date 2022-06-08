package needle

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	"io"
	"os"
	"testing"

	"github.com/chrislusf/seaweedfs/weed/storage/types"
)

func TestPageRead(t *testing.T) {
	baseFileName := "43"
	offset := int64(8)
	size := types.Size(1153890) // actual file size 1153862

	datFile, err := os.OpenFile(baseFileName+".dat", os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("Open Volume Data File [ERROR]: %v", err)
	}
	datBackend := backend.NewDiskFile(datFile)
	defer datBackend.Close()
	{
		n := new(Needle)

		bytes, err := ReadNeedleBlob(datBackend, offset, size, Version3)
		if err != nil {
			t.Fatalf("readNeedleBlob: %v", err)
		}
		if err = n.ReadBytes(bytes, offset, size, Version3); err != nil {
			t.Fatalf("readNeedleBlob: %v", err)
		}

		fmt.Printf("bytes len %d\n", len(bytes))
		fmt.Printf("name %s size %d\n", n.Name, n.Size)

		fmt.Printf("id %d\n", n.Id)
		fmt.Printf("DataSize %d\n", n.DataSize)
		fmt.Printf("Flags %v\n", n.Flags)
		fmt.Printf("NameSize %d\n", n.NameSize)
		fmt.Printf("MimeSize %d\n", n.MimeSize)
		fmt.Printf("PairsSize %d\n", n.PairsSize)
		fmt.Printf("LastModified %d\n", n.LastModified)
		fmt.Printf("AppendAtNs %d\n", n.AppendAtNs)
		fmt.Printf("Checksum %d\n", n.Checksum)
	}

	{
		n, bytes, bodyLength, err := ReadNeedleHeader(datBackend, Version3, offset)
		if err != nil {
			t.Fatalf("ReadNeedleHeader: %v", err)
		}
		fmt.Printf("bytes len %d\n", len(bytes))
		fmt.Printf("name %s size %d bodyLength:%d\n", n.Name, n.Size, bodyLength)
	}

	{
		n := new(Needle)
		err := n.ReadNeedleMeta(datBackend, offset, size, Version3)
		if err != nil {
			t.Fatalf("ReadNeedleHeader: %v", err)
		}
		fmt.Printf("name %s size %d\n", n.Name, n.Size)
		fmt.Printf("id %d\n", n.Id)
		fmt.Printf("DataSize %d\n", n.DataSize)
		fmt.Printf("Flags %v\n", n.Flags)
		fmt.Printf("NameSize %d\n", n.NameSize)
		fmt.Printf("MimeSize %d\n", n.MimeSize)
		fmt.Printf("PairsSize %d\n", n.PairsSize)
		fmt.Printf("LastModified %d\n", n.LastModified)
		fmt.Printf("AppendAtNs %d\n", n.AppendAtNs)
		fmt.Printf("Checksum %d\n", n.Checksum)

		buf := make([]byte, 1024)
		if err = n.ReadNeedleDataInto(datBackend, offset, buf, io.Discard, 0, int64(n.DataSize)); err != nil {
			t.Fatalf("ReadNeedleDataInto: %v", err)
		}

	}

}
