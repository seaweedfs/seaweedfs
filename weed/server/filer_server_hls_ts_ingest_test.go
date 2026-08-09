package weed_server

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
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
