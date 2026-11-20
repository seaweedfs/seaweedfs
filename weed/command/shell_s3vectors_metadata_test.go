package command

import (
	"bytes"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/vector_pb"
)

func TestPrintMetadataKeys(t *testing.T) {
	var out bytes.Buffer
	if err := printMetadataKeys(&out, &vector_pb.MetadataKeysResponse{Keys: []*vector_pb.MetadataKey{
		{Key: "lang", State: "requested", Rows: 10},
		{Key: "author", State: "ready", Column: "_mk_0", IndexedRows: 75, Rows: 100, Segments: 2, IndexBytes: 3 << 20},
	}}); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 3 || !strings.Contains(lines[1], "requested") || !strings.Contains(lines[2], "75%") || !strings.Contains(lines[2], "3.0 MiB") {
		t.Fatalf("%q", out.String())
	}
	out.Reset()
	_ = printMetadataKeys(&out, &vector_pb.MetadataKeysResponse{})
	if !strings.Contains(out.String(), "no indexed metadata keys") {
		t.Fatal(out.String())
	}
}
