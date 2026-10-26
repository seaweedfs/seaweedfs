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
