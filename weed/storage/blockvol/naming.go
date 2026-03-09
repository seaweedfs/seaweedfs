package blockvol

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
)

var reInvalidFilename = regexp.MustCompile(`[^a-z0-9._-]`)
var reInvalidIQN = regexp.MustCompile(`[^a-z0-9.\-]`)

// SanitizeFilename normalizes a volume name for use as a filename.
// Lowercases, replaces invalid chars with '-'.
func SanitizeFilename(name string) string {
	return reInvalidFilename.ReplaceAllString(strings.ToLower(name), "-")
}

// BuildNQN constructs an NVMe NQN from a prefix and volume name.
// The prefix must already include the separator (e.g. "nqn.2024-01.com.seaweedfs:vol.").
// This is the single source of truth for NQN construction — used by both
// the volume server (BlockService) and the CSI driver (VolumeManager/nodeServer).
func BuildNQN(prefix, name string) string {
	return prefix + SanitizeIQN(name)
}

// SanitizeIQN normalizes a CSI volume ID for use in an IQN.
// Lowercases, replaces invalid chars with '-', truncates to 64 chars.
// When truncation is needed, a hash suffix is appended to preserve uniqueness.
func SanitizeIQN(name string) string {
	s := strings.ToLower(name)
	s = reInvalidIQN.ReplaceAllString(s, "-")
	if len(s) > 64 {
		h := sha256.Sum256([]byte(name))
		suffix := hex.EncodeToString(h[:4]) // 8 hex chars
		s = s[:64-1-len(suffix)] + "-" + suffix
	}
	return s
}
