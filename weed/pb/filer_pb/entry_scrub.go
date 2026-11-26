package filer_pb

import "google.golang.org/protobuf/proto"

// ScrubCipherKeys returns a copy of entry with every chunk's CipherKey
// cleared, so logging it cannot leak per-chunk encryption keys.
func ScrubCipherKeys(entry *Entry) *Entry {
	if entry == nil {
		return nil
	}
	clone := proto.Clone(entry).(*Entry)
	clearCipherKeys(clone)
	return clone
}

// ScrubEventCipherKeys is ScrubCipherKeys for both entries of an event.
func ScrubEventCipherKeys(event *EventNotification) *EventNotification {
	if event == nil {
		return nil
	}
	clone := proto.Clone(event).(*EventNotification)
	clearCipherKeys(clone.OldEntry)
	clearCipherKeys(clone.NewEntry)
	return clone
}

func clearCipherKeys(entry *Entry) {
	for _, chunk := range entry.GetChunks() {
		if chunk != nil {
			chunk.CipherKey = nil
		}
	}
}
