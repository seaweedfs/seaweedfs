package format

import (
	"strings"
	"testing"
)

func TestLayoutEncodeDecodeRoundTrip(t *testing.T) {
	layout := &Layout{
		Format:      "hls-ts",
		ExtentSizes: []int64{188 * 3, 188 * 2, 188 * 7},
		Align:       188,
		Payload:     []byte{1, 2, 3},
	}
	encoded, err := layout.Encode()
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	decoded, err := DecodeLayout(encoded)
	if err != nil {
		t.Fatalf("DecodeLayout() error = %v", err)
	}
	if decoded.Format != layout.Format || decoded.Align != layout.Align {
		t.Fatalf("decoded = %+v, want %+v", decoded, layout)
	}
	if len(decoded.ExtentSizes) != len(layout.ExtentSizes) {
		t.Fatalf("extent count = %d, want %d", len(decoded.ExtentSizes), len(layout.ExtentSizes))
	}
	for i := range layout.ExtentSizes {
		if decoded.ExtentSizes[i] != layout.ExtentSizes[i] {
			t.Fatalf("extent %d = %d, want %d", i, decoded.ExtentSizes[i], layout.ExtentSizes[i])
		}
	}
	if string(decoded.Payload) != string(layout.Payload) {
		t.Fatalf("payload = %v, want %v", decoded.Payload, layout.Payload)
	}
}

func TestDecodeLayoutRejectsCorruptInput(t *testing.T) {
	layout := &Layout{Format: "parquet", ExtentSizes: []int64{10, 20}, Align: 1}
	encoded, err := layout.Encode()
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	for cut := 0; cut < len(encoded); cut++ {
		if _, err := DecodeLayout(encoded[:cut]); err == nil {
			t.Fatalf("DecodeLayout() accepted truncation at %d", cut)
		}
	}
	if _, err := DecodeLayout(append(append([]byte{}, encoded...), 0)); err == nil {
		t.Fatalf("DecodeLayout() accepted trailing bytes")
	}
}

func TestLayoutValidate(t *testing.T) {
	tests := []struct {
		name     string
		layout   Layout
		fileSize int64
		wantErr  string
	}{
		{"valid", Layout{Format: "x", ExtentSizes: []int64{5, 5}, Align: 1}, 10, ""},
		{"skip size check", Layout{Format: "x", ExtentSizes: []int64{5}, Align: 1}, -1, ""},
		{"wrong total", Layout{Format: "x", ExtentSizes: []int64{5, 5}, Align: 1}, 11, "but the file has"},
		{"zero extent", Layout{Format: "x", ExtentSizes: []int64{5, 0}, Align: 1}, -1, "invalid size"},
		{"no extents", Layout{Format: "x", Align: 1}, -1, "no extents"},
		{"bad align", Layout{Format: "x", ExtentSizes: []int64{5}, Align: 0}, -1, "align"},
		{"no name", Layout{ExtentSizes: []int64{5}, Align: 1}, -1, "format name"},
	}
	for _, test := range tests {
		err := test.layout.Validate(test.fileSize)
		if test.wantErr == "" {
			if err != nil {
				t.Fatalf("%s: Validate() error = %v", test.name, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), test.wantErr) {
			t.Fatalf("%s: Validate() error = %v, want %q", test.name, err, test.wantErr)
		}
	}
}

func TestExtentRange(t *testing.T) {
	layout := &Layout{Format: "x", ExtentSizes: []int64{10, 20, 30}, Align: 1}
	offset, size, ok := layout.ExtentRange(1)
	if !ok || offset != 10 || size != 20 {
		t.Fatalf("ExtentRange(1) = (%d, %d, %v), want (10, 20, true)", offset, size, ok)
	}
	if _, _, ok := layout.ExtentRange(3); ok {
		t.Fatalf("ExtentRange(3) accepted out-of-range index")
	}
	if _, _, ok := layout.ExtentRange(-1); ok {
		t.Fatalf("ExtentRange(-1) accepted negative index")
	}
}

// collectChunks walks the cutter the way the upload loop does.
func collectChunks(t *testing.T, cutter *Cutter) [][2]int64 {
	t.Helper()
	var chunks [][2]int64
	var offset int64
	for {
		size := cutter.NextChunkSize(offset)
		if size <= 0 {
			return chunks
		}
		chunks = append(chunks, [2]int64{offset, size})
		offset += size
	}
}

func TestCutterKeepsExtentBoundaries(t *testing.T) {
	layout := &Layout{Format: "x", ExtentSizes: []int64{5, 4}, Align: 1}
	chunks := collectChunks(t, layout.Cutter(16))
	want := [][2]int64{{0, 5}, {5, 4}}
	if len(chunks) != len(want) {
		t.Fatalf("chunks = %v, want %v", chunks, want)
	}
	for i := range want {
		if chunks[i] != want[i] {
			t.Fatalf("chunk %d = %v, want %v", i, chunks[i], want[i])
		}
	}
}

func TestCutterSplitsOversizedExtentsOnAlign(t *testing.T) {
	// maxChunkSize 5 with align 2 quantizes down to 4-byte interior cuts.
	layout := &Layout{Format: "x", ExtentSizes: []int64{10, 3}, Align: 2}
	chunks := collectChunks(t, layout.Cutter(5))
	want := [][2]int64{{0, 4}, {4, 4}, {8, 2}, {10, 3}}
	if len(chunks) != len(want) {
		t.Fatalf("chunks = %v, want %v", chunks, want)
	}
	for i := range want {
		if chunks[i] != want[i] {
			t.Fatalf("chunk %d = %v, want %v", i, chunks[i], want[i])
		}
	}
}

func TestCutterAlignLargerThanChunkLimit(t *testing.T) {
	// Align above maxChunkSize still cuts on whole atoms.
	layout := &Layout{Format: "x", ExtentSizes: []int64{20}, Align: 8}
	chunks := collectChunks(t, layout.Cutter(5))
	want := [][2]int64{{0, 8}, {8, 8}, {16, 4}}
	for i := range want {
		if chunks[i] != want[i] {
			t.Fatalf("chunk %d = %v, want %v", i, chunks[i], want[i])
		}
	}
}

func TestCutterUnlimitedKeepsOneChunkPerExtent(t *testing.T) {
	layout := &Layout{Format: "x", ExtentSizes: []int64{10, 3}, Align: 188}
	chunks := collectChunks(t, layout.Cutter(0))
	want := [][2]int64{{0, 10}, {10, 3}}
	if len(chunks) != len(want) {
		t.Fatalf("chunks = %v, want %v", chunks, want)
	}
}
