package blockvol

import (
	"errors"
	"testing"
)

func TestLBAValidation(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "lba_within_bounds", run: testLBAWithinBounds},
		{name: "lba_last_block", run: testLBALastBlock},
		{name: "lba_out_of_bounds", run: testLBAOutOfBounds},
		{name: "lba_write_spans_end", run: testLBAWriteSpansEnd},
		{name: "lba_alignment", run: testLBAAlignment},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

const (
	testVolSize   = 100 * 1024 * 1024 * 1024 // 100GB
	testBlockSize = 4096
)

func testLBAWithinBounds(t *testing.T) {
	err := ValidateLBA(0, testVolSize, testBlockSize)
	if err != nil {
		t.Errorf("LBA=0 should be valid: %v", err)
	}

	err = ValidateLBA(1000, testVolSize, testBlockSize)
	if err != nil {
		t.Errorf("LBA=1000 should be valid: %v", err)
	}
}

func testLBALastBlock(t *testing.T) {
	maxLBA := uint64(testVolSize/testBlockSize) - 1
	err := ValidateLBA(maxLBA, testVolSize, testBlockSize)
	if err != nil {
		t.Errorf("last LBA=%d should be valid: %v", maxLBA, err)
	}
}

func testLBAOutOfBounds(t *testing.T) {
	maxLBA := uint64(testVolSize / testBlockSize)
	err := ValidateLBA(maxLBA, testVolSize, testBlockSize)
	if !errors.Is(err, ErrLBAOutOfBounds) {
		t.Errorf("LBA=%d (one past end): expected ErrLBAOutOfBounds, got %v", maxLBA, err)
	}

	err = ValidateLBA(maxLBA+1000, testVolSize, testBlockSize)
	if !errors.Is(err, ErrLBAOutOfBounds) {
		t.Errorf("LBA far past end: expected ErrLBAOutOfBounds, got %v", err)
	}
}

func testLBAWriteSpansEnd(t *testing.T) {
	maxLBA := uint64(testVolSize / testBlockSize)
	// Write 2 blocks starting at last LBA -- spans past end.
	lastLBA := maxLBA - 1
	err := ValidateWrite(lastLBA, 2*testBlockSize, testVolSize, testBlockSize)
	if !errors.Is(err, ErrWritePastEnd) {
		t.Errorf("write spanning end: expected ErrWritePastEnd, got %v", err)
	}

	// Write 1 block at last LBA -- should succeed.
	err = ValidateWrite(lastLBA, testBlockSize, testVolSize, testBlockSize)
	if err != nil {
		t.Errorf("write at last LBA should succeed: %v", err)
	}
}

func testLBAAlignment(t *testing.T) {
	err := ValidateWrite(0, 4096, testVolSize, testBlockSize)
	if err != nil {
		t.Errorf("aligned write should succeed: %v", err)
	}

	err = ValidateWrite(0, 4000, testVolSize, testBlockSize)
	if !errors.Is(err, ErrAlignment) {
		t.Errorf("unaligned write: expected ErrAlignment, got %v", err)
	}

	err = ValidateWrite(0, 1, testVolSize, testBlockSize)
	if !errors.Is(err, ErrAlignment) {
		t.Errorf("1-byte write: expected ErrAlignment, got %v", err)
	}
}
