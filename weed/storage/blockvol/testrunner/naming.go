package testrunner

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
)

// Naming helpers for IQN/NQN construction.
// Copied from blockvol/naming.go to decouple the testrunner from the engine package.
// The engine remains the source of truth for production code; these copies are
// used only by the test runner to avoid importing the engine.

var reInvalidIQN = regexp.MustCompile(`[^a-z0-9.\-]`)

// SanitizeIQN normalizes a name for use in an IQN.
// Lowercases, replaces invalid chars with '-', truncates to 64 chars.
func SanitizeIQN(name string) string {
	s := strings.ToLower(name)
	s = reInvalidIQN.ReplaceAllString(s, "-")
	if len(s) > 64 {
		h := sha256.Sum256([]byte(name))
		suffix := hex.EncodeToString(h[:4])
		s = s[:64-1-len(suffix)] + "-" + suffix
	}
	return s
}

// BuildNQN constructs an NVMe NQN from a prefix and volume name.
func BuildNQN(prefix, name string) string {
	return prefix + SanitizeIQN(name)
}
