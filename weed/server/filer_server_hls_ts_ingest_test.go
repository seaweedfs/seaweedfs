package weed_server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"

	media_hls "github.com/seaweedfs/seaweedfs/weed/media/hls"
)

func TestHlsTsIngestErrorStatus(t *testing.T) {
	if got := hlsTsIngestErrorStatus(errors.New("invalid transport stream")); got != http.StatusBadRequest {
		t.Fatalf("client error status = %d, want %d", got, http.StatusBadRequest)
	}

	storageErr := &hlsTsStorageError{err: errors.New("volume upload failed")}
	wrapped := fmt.Errorf("process segment chunk: %w", storageErr)
	if got := hlsTsIngestErrorStatus(wrapped); got != http.StatusInternalServerError {
		t.Fatalf("storage error status = %d, want %d", got, http.StatusInternalServerError)
	}
}

func TestEncodeHlsTsMetadataProducesValidJSON(t *testing.T) {
	metadata := &media_hls.Metadata{
		Version:        1,
		TargetDuration: 6,
		MediaSequence:  7,
		Segments: []media_hls.Segment{
			{Offset: 0, Size: 188, Duration: 5.5},
		},
	}

	encoded, err := encodeHlsTsMetadata(metadata)
	if err != nil {
		t.Fatalf("encode metadata: %v", err)
	}
	if bytes.Contains(encoded, []byte{'\n'}) {
		t.Fatalf("encoded metadata contains an unexpected newline: %q", encoded)
	}

	decoded := &media_hls.Metadata{}
	if err := json.Unmarshal(encoded, decoded); err != nil {
		t.Fatalf("decode metadata: %v; JSON = %q", err, encoded)
	}
	if decoded.Version != metadata.Version || decoded.TargetDuration != metadata.TargetDuration || decoded.MediaSequence != metadata.MediaSequence || len(decoded.Segments) != 1 || decoded.Segments[0] != metadata.Segments[0] {
		t.Fatalf("metadata round trip = %+v, want %+v", decoded, metadata)
	}
}
