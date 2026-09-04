package filer

import (
	"errors"
	"fmt"
	"testing"
)

// The path in a non-empty-folder failure is the client's, so the marker has to
// lead the message for a caller reading it off the wire to trust it.
func TestNonEmptyFolderClassification(t *testing.T) {
	err := fmt.Errorf("%w: %s", ErrNonEmptyFolder, "/buckets/b/photos")
	if !IsNonEmptyFolderError(err) {
		t.Errorf("expected the sentinel to be recognized: %v", err)
	}
	if !errors.Is(DeleteEntryError(err.Error()), ErrNonEmptyFolder) {
		t.Errorf("expected the wire text to classify: %v", err)
	}

	// an entry named after the marker cannot forge one: every other delete
	// failure the filer builds leads with its own wrapper
	spoofed := "delete file /buckets/b/" + MsgFailDelNonEmptyFolder + ": filer store delete: disk full"
	if errors.Is(DeleteEntryError(spoofed), ErrNonEmptyFolder) {
		t.Errorf("expected no forgery from the entry name: %v", spoofed)
	}
	if IsNonEmptyFolderError(errors.New(spoofed)) {
		t.Errorf("expected no forgery from the entry name: %v", spoofed)
	}
}
