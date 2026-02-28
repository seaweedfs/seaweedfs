package blockvol

import (
	"errors"
	"fmt"
)

var (
	ErrLBAOutOfBounds = errors.New("blockvol: LBA out of bounds")
	ErrWritePastEnd   = errors.New("blockvol: write extends past volume end")
	ErrAlignment      = errors.New("blockvol: data length not aligned to block size")
)

// ValidateLBA checks that lba is within the volume's logical address space.
func ValidateLBA(lba uint64, volumeSize uint64, blockSize uint32) error {
	maxLBA := volumeSize / uint64(blockSize)
	if lba >= maxLBA {
		return fmt.Errorf("%w: lba=%d, max=%d", ErrLBAOutOfBounds, lba, maxLBA-1)
	}
	return nil
}

// ValidateWrite checks that a write at lba with dataLen bytes fits within
// the volume and that dataLen is aligned to blockSize.
func ValidateWrite(lba uint64, dataLen uint32, volumeSize uint64, blockSize uint32) error {
	if err := ValidateLBA(lba, volumeSize, blockSize); err != nil {
		return err
	}
	if dataLen%blockSize != 0 {
		return fmt.Errorf("%w: dataLen=%d, blockSize=%d", ErrAlignment, dataLen, blockSize)
	}
	blocksNeeded := uint64(dataLen / blockSize)
	maxLBA := volumeSize / uint64(blockSize)
	if lba+blocksNeeded > maxLBA {
		return fmt.Errorf("%w: lba=%d, blocks=%d, max=%d", ErrWritePastEnd, lba, blocksNeeded, maxLBA)
	}
	return nil
}
