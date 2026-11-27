package filer_pb

import (
	"fmt"
	"testing"
)

func TestScrubCipherKeys(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	entry := &Entry{
		Name: "f",
		Chunks: []*FileChunk{
			{FileId: "1,aaa", CipherKey: key},
			{FileId: "2,bbb"},
		},
	}

	scrubbed := ScrubCipherKeys(entry)

	for i, chunk := range scrubbed.Chunks {
		if len(chunk.CipherKey) != 0 {
			t.Fatalf("chunk %d still carries a cipher key", i)
		}
	}
	if len(entry.Chunks[0].CipherKey) != len(key) {
		t.Fatal("scrubbing mutated the original entry")
	}
	if got := fmt.Sprintf("%v", scrubbed); got == fmt.Sprintf("%v", entry) {
		t.Fatal("scrubbed entry prints identically to the original")
	}
	if ScrubCipherKeys(nil) != nil {
		t.Fatal("nil entry should scrub to nil")
	}
}

func TestScrubEventCipherKeys(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	event := &EventNotification{
		OldEntry:      &Entry{Name: "a", Chunks: []*FileChunk{{FileId: "1,aaa", CipherKey: key}}},
		NewEntry:      &Entry{Name: "b", Chunks: []*FileChunk{{FileId: "2,bbb", CipherKey: key}}},
		NewParentPath: "/dir",
		Signatures:    []int32{7},
	}

	scrubbed := ScrubEventCipherKeys(event)

	for _, entry := range []*Entry{scrubbed.OldEntry, scrubbed.NewEntry} {
		if len(entry.Chunks[0].CipherKey) != 0 {
			t.Fatalf("%s still carries a cipher key", entry.Name)
		}
	}
	if scrubbed.NewParentPath != "/dir" || len(scrubbed.Signatures) != 1 || scrubbed.NewEntry.Chunks[0].FileId != "2,bbb" {
		t.Fatalf("scrubbing dropped event fields: %v", scrubbed)
	}
	if len(event.OldEntry.Chunks[0].CipherKey) != len(key) || len(event.NewEntry.Chunks[0].CipherKey) != len(key) {
		t.Fatal("scrubbing mutated the original event")
	}
	if ScrubEventCipherKeys(nil) != nil {
		t.Fatal("nil event should scrub to nil")
	}
	if got := ScrubEventCipherKeys(&EventNotification{}); got.OldEntry != nil || got.NewEntry != nil {
		t.Fatal("event without entries gained entries")
	}
}
