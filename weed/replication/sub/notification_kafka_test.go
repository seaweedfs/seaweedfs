package sub

import (
	"testing"
	"time"
)

func newTestProgress() *KafkaProgress {
	return &KafkaProgress{
		Topic:            "test",
		PartitionOffsets: make(map[int32]int64),
		failedOffsets:    make(map[int32]int64),
		lastSaveTime:     time.Now(),
		// large enough that no test call reaches saveProgress and touches disk
		offsetSaveIntervalSeconds: 3600,
	}
}

// TestKafkaProgressHoldsOffsetAtFailure verifies that a message that failed to
// replicate keeps the committed offset behind it, so a restart redelivers it
// rather than resuming after it.
func TestKafkaProgressHoldsOffsetAtFailure(t *testing.T) {
	progress := newTestProgress()

	if err := progress.setOffset(0, 10); err != nil {
		t.Fatalf("setOffset: %v", err)
	}
	if got := progress.PartitionOffsets[0]; got != 10 {
		t.Fatalf("offset = %d after a replicated message, want 10", got)
	}

	progress.markFailed(0, 11)

	if err := progress.setOffset(0, 12); err != nil {
		t.Fatalf("setOffset: %v", err)
	}
	if got := progress.PartitionOffsets[0]; got != 10 {
		t.Fatalf("offset = %d after a later success, want it held at 10", got)
	}

	// a failure in one partition must not stall the others
	if err := progress.setOffset(1, 5); err != nil {
		t.Fatalf("setOffset: %v", err)
	}
	if got := progress.PartitionOffsets[1]; got != 5 {
		t.Fatalf("offset = %d in an unaffected partition, want 5", got)
	}
}

// TestKafkaProgressKeepsOldestFailure verifies that the hold point is the
// oldest failed offset, not the most recent one.
func TestKafkaProgressKeepsOldestFailure(t *testing.T) {
	progress := newTestProgress()

	progress.markFailed(0, 20)
	progress.markFailed(0, 11)
	progress.markFailed(0, 30)

	if got := progress.failedOffsets[0]; got != 11 {
		t.Fatalf("held at offset %d, want the oldest failure 11", got)
	}

	if err := progress.setOffset(0, 10); err != nil {
		t.Fatalf("setOffset: %v", err)
	}
	if got := progress.PartitionOffsets[0]; got != 10 {
		t.Fatalf("offset = %d, want an offset before the failure to still commit", got)
	}
}
