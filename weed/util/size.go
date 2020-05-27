package util

import "fmt"

// BytesToHumanReadable exposes the bytesToHumanReadble.
// It returns the converted human readable representation of the bytes
func BytesToHumanReadable(b uint64) string {
	const unit = 1024

	if b < unit {
		return fmt.Sprintf("%d", b)
	}

	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%d(%.2f %ciB)", b, float64(b)/float64(div), "KMGTPE"[exp])
}
