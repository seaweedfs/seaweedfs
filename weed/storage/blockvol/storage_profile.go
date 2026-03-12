package blockvol

import (
	"errors"
	"fmt"
	"strings"
)

// StorageProfile controls the data layout strategy for a block volume.
//   - ProfileSingle (0): single-server, one file (default, backward-compat)
//   - ProfileStriped (1): multi-server striping (reserved, not yet implemented)
type StorageProfile uint8

const (
	ProfileSingle  StorageProfile = 0 // zero-value = backward compat
	ProfileStriped StorageProfile = 1 // reserved — rejected at creation time
)

var (
	ErrInvalidStorageProfile = errors.New("blockvol: invalid storage profile")
	ErrStripedNotImplemented = errors.New("blockvol: striped profile is not yet implemented")
)

// ParseStorageProfile converts a string to StorageProfile.
// Empty string is treated as "single" for backward compatibility.
// Parsing is case-insensitive.
func ParseStorageProfile(s string) (StorageProfile, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "single":
		return ProfileSingle, nil
	case "striped":
		return ProfileStriped, nil
	default:
		return 0, fmt.Errorf("%w: %q", ErrInvalidStorageProfile, s)
	}
}

// String returns the canonical string representation.
func (p StorageProfile) String() string {
	switch p {
	case ProfileSingle:
		return "single"
	case ProfileStriped:
		return "striped"
	default:
		return fmt.Sprintf("unknown(%d)", p)
	}
}
