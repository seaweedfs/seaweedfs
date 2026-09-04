// Package formattest is the conformance kit format adapters must pass:
// indexers parse attacker-controlled bytes inside a storage daemon, so they
// must never panic and every layout they accept must validate.
package formattest

import (
	"bytes"
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/format"
)

// IndexTruncations feeds progressively truncated copies of a valid file to the
// indexer. Any outcome is acceptable except a panic or an invalid layout.
func IndexTruncations(t *testing.T, indexer format.Indexer, data []byte) {
	t.Helper()
	for i := 0; i <= 16; i++ {
		size := int64(len(data) * i / 16)
		layout, err := indexer.Index(context.Background(), bytes.NewReader(data[:size]), size)
		if err != nil {
			continue
		}
		if validateErr := layout.Validate(size); validateErr != nil {
			t.Fatalf("Index() at %d/%d bytes returned an invalid layout: %v", size, len(data), validateErr)
		}
	}
}

// SidecarTruncations does the same for sidecar index documents.
func SidecarTruncations(t *testing.T, indexer format.SidecarIndexer, sidecar []byte) {
	t.Helper()
	for i := 0; i <= 16; i++ {
		layout, err := indexer.IndexSidecar(sidecar[:len(sidecar)*i/16])
		if err != nil {
			continue
		}
		if validateErr := layout.Validate(-1); validateErr != nil {
			t.Fatalf("IndexSidecar() at %d/16 returned an invalid layout: %v", i, validateErr)
		}
	}
}

// EncodeRoundTrip checks that a layout survives the persistence codec.
func EncodeRoundTrip(t *testing.T, layout *format.Layout) {
	t.Helper()
	encoded, err := layout.Encode()
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	decoded, err := format.DecodeLayout(encoded)
	if err != nil {
		t.Fatalf("DecodeLayout() error = %v", err)
	}
	if decoded.Format != layout.Format || decoded.Align != layout.Align ||
		len(decoded.ExtentSizes) != len(layout.ExtentSizes) || !bytes.Equal(decoded.Payload, layout.Payload) {
		t.Fatalf("decoded layout %+v differs from %+v", decoded, layout)
	}
	for i := range layout.ExtentSizes {
		if decoded.ExtentSizes[i] != layout.ExtentSizes[i] {
			t.Fatalf("extent %d = %d, want %d", i, decoded.ExtentSizes[i], layout.ExtentSizes[i])
		}
	}
}
