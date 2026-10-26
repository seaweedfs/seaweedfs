package filer_pb

import "google.golang.org/protobuf/proto"

// ScrubCipherKeys returns a copy of entry with every chunk's CipherKey
// cleared, so logging it cannot leak per-chunk encryption keys.
func ScrubCipherKeys(entry *Entry) *Entry {
	if entry == nil {
		return nil
	}
	clone := proto.Clone(entry).(*Entry)
	for _, chunk := range clone.Chunks {
		if chunk != nil {
			chunk.CipherKey = nil
		}
	}
	return clone
}
